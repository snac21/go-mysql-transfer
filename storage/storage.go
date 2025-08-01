/*
 * Copyright 2020-2021 the original author(https://github.com/wj596)
 *
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * </p>
 */

// Package storage 存储层模块
// 提供了多种存储后端的统一抽象，支持本地存储和分布式存储
// 主要功能：
// 1. MySQL binlog位置信息的持久化存储
// 2. 集群模式下的分布式协调存储
// 3. 支持BoltDB（本地）、ZooKeeper、etcd等多种存储后端
// 4. 提供统一的存储接口，屏蔽底层存储差异
package storage

import (
	// 错误处理
	"fmt"           // 格式化输出
	"path/filepath" // 文件路径操作
	"strings"       // 字符串操作
	"time"          // 时间处理

	"github.com/samuel/go-zookeeper/zk"   // ZooKeeper客户端
	"go.etcd.io/bbolt"                    // BoltDB嵌入式数据库
	clientv3 "go.etcd.io/etcd/client/v3"  // etcd v3客户端
	etcdlog "go.etcd.io/etcd/pkg/logutil" // etcd日志工具

	"go-mysql-transfer/global"          // 全局配置
	"go-mysql-transfer/util/byteutil"   // 字节工具
	"go-mysql-transfer/util/files"      // 文件工具
	"go-mysql-transfer/util/logagent"   // 日志代理
	"go-mysql-transfer/util/zookeepers" // ZooKeeper工具
)

// 存储相关常量定义
const (
	_boltFilePath = "db"      // BoltDB存储目录名
	_boltFileName = "data.db" // BoltDB数据文件名
	_boltFileMode = 0600      // BoltDB文件权限（仅所有者可读写）
)

// 存储相关全局变量
var (
	// BoltDB相关变量
	_positionBucket = []byte("Position")                // BoltDB中存储位置信息的桶名
	_fixPositionId  = byteutil.Uint64ToBytes(uint64(1)) // 固定的位置记录ID（转换为字节数组）
	_bolt           *bbolt.DB                           // BoltDB数据库连接实例

	// ZooKeeper相关变量
	_zkConn         *zk.Conn        // ZooKeeper连接实例
	_zkStatusSignal <-chan zk.Event // ZooKeeper状态变化信号通道
	_zkAddresses    []string        // ZooKeeper服务器地址列表

	// etcd相关变量
	_etcdConn *clientv3.Client // etcd客户端连接实例
	_etcdOps  clientv3.KV      // etcd键值操作接口
)

// Initialize 初始化存储层
// 根据配置初始化不同的存储后端：
// 1. 总是初始化BoltDB作为本地存储
// 2. 如果配置了集群模式，还会初始化ZooKeeper或etcd
// 返回初始化过程中的错误，如果有的话
func Initialize() error {
	// 初始化BoltDB本地存储（必需）
	if err := initBolt(); err != nil {
		return err
	}

	// 如果配置了ZooKeeper集群模式，初始化ZooKeeper连接
	if global.Cfg().IsZk() {
		if err := initZk(); err != nil {
			return err
		}
	}

	// 如果配置了etcd集群模式，初始化etcd连接
	if global.Cfg().IsEtcd() {
		if err := initEtcd(); err != nil {
			return err
		}
	}

	return nil
}

// initBolt 初始化BoltDB嵌入式数据库
// BoltDB是一个纯Go实现的嵌入式键值数据库，用于本地存储binlog位置信息
// 初始化过程：
// 1. 创建存储目录
// 2. 打开数据库文件
// 3. 创建存储桶（类似表）
// 返回初始化过程中的错误
func initBolt() error {
	// 构建BoltDB存储目录路径
	blotStorePath := filepath.Join(global.Cfg().DataDir, _boltFilePath)

	// 如果目录不存在则创建
	if err := files.MkdirIfNecessary(blotStorePath); err != nil {
		return fmt.Errorf("create boltdb store : %s", err.Error())
	}

	// 构建BoltDB数据文件完整路径
	boltFilePath := filepath.Join(blotStorePath, _boltFileName)

	// 打开BoltDB数据库文件
	// 如果文件不存在会自动创建
	bolt, err := bbolt.Open(boltFilePath, _boltFileMode, bbolt.DefaultOptions)
	if err != nil {
		return fmt.Errorf("open boltdb: %s", err.Error())
	}

	// 创建存储位置信息的桶（如果不存在）
	// 桶类似于关系数据库中的表概念
	err = bolt.Update(func(tx *bbolt.Tx) error {
		tx.CreateBucketIfNotExists(_positionBucket)
		return nil
	})

	// 保存数据库连接实例到全局变量
	_bolt = bolt

	return err
}

// initZk 初始化ZooKeeper连接
// ZooKeeper用于集群模式下的分布式协调和数据共享
// 初始化过程：
// 1. 解析ZooKeeper服务器地址
// 2. 建立连接
// 3. 设置认证（如果配置了）
// 4. 创建必要的目录结构
// 返回初始化过程中的错误
func initZk() error {
	// 设置ZooKeeper日志代理
	option := zk.WithLogger(logagent.NewZkLoggerAgent())

	// 解析ZooKeeper服务器地址列表（逗号分隔）
	list := strings.Split(global.Cfg().Cluster.ZkAddrs, ",")

	// 建立ZooKeeper连接
	// 参数：服务器列表、会话超时时间、选项
	conn, sig, err := zk.Connect(list, time.Second, option)
	if err != nil {
		return err
	}

	// 如果配置了认证信息，添加认证
	if global.Cfg().Cluster.ZkAuthentication != "" {
		err = conn.AddAuth("digest", []byte(global.Cfg().Cluster.ZkAuthentication))
		if err != nil {
			return err
		}
	}

	// 创建根目录（如果不存在）
	// 用于存储应用相关的所有数据
	err = zookeepers.CreateDirIfNecessary(global.Cfg().ZkRootDir(), conn)
	if err != nil {
		return err
	}

	// 创建集群目录（如果不存在）
	// 用于存储集群节点信息
	err = zookeepers.CreateDirIfNecessary(global.Cfg().ZkClusterDir(), conn)
	if err != nil {
		return err
	}

	// 保存连接信息到全局变量
	_zkAddresses = list   // 服务器地址列表
	_zkConn = conn        // 连接实例
	_zkStatusSignal = sig // 状态变化信号通道

	return nil
}

// initEtcd 初始化etcd连接
// etcd是一个分布式键值存储系统，用于集群模式下的配置共享和服务发现
// 初始化过程：
// 1. 配置日志
// 2. 解析etcd服务器地址
// 3. 创建客户端连接
// 4. 初始化键值操作接口
// 返回初始化过程中的错误
func initEtcd() error {
	// 配置etcd的日志输出
	etcdlog.DefaultZapLoggerConfig = logagent.EtcdZapLoggerConfig()
	clientv3.SetLogger(logagent.NewEtcdLoggerAgent())

	// 解析etcd服务器地址列表（逗号分隔）
	list := strings.Split(global.Cfg().Cluster.EtcdAddrs, ",")

	// 构建etcd客户端配置
	config := clientv3.Config{
		Endpoints:   list,                              // etcd服务器端点列表
		Username:    global.Cfg().Cluster.EtcdUser,     // 用户名（如果需要认证）
		Password:    global.Cfg().Cluster.EtcdPassword, // 密码（如果需要认证）
		DialTimeout: 1 * time.Second,                   // 连接超时时间
	}

	// 创建etcd客户端连接
	client, err := clientv3.New(config)
	if err != nil {
		return err
	}

	// 保存连接实例到全局变量
	_etcdConn = client                   // 客户端连接
	_etcdOps = clientv3.NewKV(_etcdConn) // 键值操作接口

	return nil
}

// ZKConn 获取ZooKeeper连接实例
// 返回全局的ZooKeeper连接，供其他模块使用
func ZKConn() *zk.Conn {
	return _zkConn
}

// ZKStatusSignal 获取ZooKeeper状态变化信号通道
// 返回只读通道，用于监听ZooKeeper连接状态变化事件
// 如连接断开、重连成功等
func ZKStatusSignal() <-chan zk.Event {
	return _zkStatusSignal
}

// ZKAddresses 获取ZooKeeper服务器地址列表
// 返回配置的ZooKeeper服务器地址数组
func ZKAddresses() []string {
	return _zkAddresses
}

// EtcdConn 获取etcd客户端连接实例
// 返回全局的etcd客户端连接，供其他模块使用
func EtcdConn() *clientv3.Client {
	return _etcdConn
}

// EtcdOps 获取etcd键值操作接口
// 返回etcd的KV操作接口，用于执行键值存储操作
func EtcdOps() clientv3.KV {
	return _etcdOps
}

// Close 关闭所有存储连接
// 在应用程序退出时调用，确保所有存储连接被正确关闭
// 防止资源泄露和数据丢失
func Close() {
	// 关闭BoltDB连接
	if _bolt != nil {
		_bolt.Close()
	}

	// 关闭ZooKeeper连接
	if _zkConn != nil {
		_zkConn.Close()
	}

	// 关闭etcd连接
	if _etcdConn != nil {
		_etcdConn.Close()
	}
}
