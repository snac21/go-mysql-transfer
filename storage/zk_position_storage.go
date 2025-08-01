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

// Package storage ZooKeeper位置存储实现模块
// 使用ZooKeeper分布式协调服务存储MySQL binlog位置信息
// ZooKeeper提供强一致性和原子操作，适合集群部署场景
package storage

import (
	"encoding/json" // JSON序列化

	"github.com/go-mysql-org/go-mysql/mysql" // MySQL相关类型

	"go-mysql-transfer/global"          // 全局配置
	"go-mysql-transfer/util/zookeepers" // ZooKeeper工具函数
)

// zkPositionStorage ZooKeeper位置存储实现
// 实现了PositionStorage接口，使用ZooKeeper作为存储后端
// 适用于集群部署模式，多个节点可以共享同一个位置信息
// ZooKeeper的版本控制机制确保了并发更新的安全性
type zkPositionStorage struct {
	// 空结构体，所有操作通过全局ZooKeeper连接进行
}

// Initialize 初始化ZooKeeper位置存储
// 创建必要的ZooKeeper节点结构：
// 1. 位置信息节点（存储binlog位置）
// 2. 集群节点目录（用于节点注册和发现）
// 返回初始化过程中的错误
func (s *zkPositionStorage) Initialize() error {
	// 创建空的位置记录并序列化为JSON
	pos, err := json.Marshal(mysql.Position{})
	if err != nil {
		return err // JSON序列化失败
	}

	// 创建位置信息节点（如果不存在）
	// 同时设置初始数据为空的位置信息
	err = zookeepers.CreateDirWithDataIfNecessary(global.Cfg().ZkPositionDir(), pos, _zkConn)
	if err != nil {
		return err // ZooKeeper节点创建失败
	}

	// 创建集群节点目录（如果不存在）
	// 用于集群中各节点的注册和服务发现
	err = zookeepers.CreateDirIfNecessary(global.Cfg().ZkNodesDir(), _zkConn)
	return err
}

// Save 保存binlog位置信息到ZooKeeper
// pos: 要保存的MySQL binlog位置信息
// 使用ZooKeeper的版本控制机制确保原子更新
// 先获取当前版本，然后基于该版本进行更新
// 返回保存过程中的错误
func (s *zkPositionStorage) Save(pos mysql.Position) error {
	// 获取当前节点的数据和状态信息
	// stat包含版本号，用于乐观锁控制
	_, stat, err := _zkConn.Get(global.Cfg().ZkPositionDir())
	if err != nil {
		return err // 获取节点状态失败
	}

	// 将位置信息序列化为JSON格式
	data, err := json.Marshal(pos)
	if err != nil {
		return err // JSON序列化失败
	}

	// 基于当前版本更新节点数据
	// 如果版本不匹配（其他节点已更新），操作会失败
	// 这确保了并发更新的安全性
	_, err = _zkConn.Set(global.Cfg().ZkPositionDir(), data, stat.Version)

	return err
}

// Get 从ZooKeeper获取当前保存的binlog位置信息
// 返回位置信息和可能的错误
// 从ZooKeeper读取JSON数据并反序列化为Position结构
func (s *zkPositionStorage) Get() (mysql.Position, error) {
	var entity mysql.Position // 用于存储反序列化后的位置信息

	// 从ZooKeeper获取位置数据
	// 返回值包括：数据内容、状态信息、错误
	data, _, err := _zkConn.Get(global.Cfg().ZkPositionDir())
	if err != nil {
		return entity, err // ZooKeeper读取失败
	}

	// 将JSON数据反序列化为Position结构
	err = json.Unmarshal(data, &entity)

	return entity, err
}
