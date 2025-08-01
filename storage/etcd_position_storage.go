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

// Package storage etcd位置存储实现模块
// 使用etcd分布式键值存储系统存储MySQL binlog位置信息
// etcd提供强一致性保证，适合集群部署场景下的位置信息共享
package storage

import (
	"encoding/json" // JSON序列化

	"github.com/go-mysql-org/go-mysql/mysql" // MySQL相关类型

	"go-mysql-transfer/global"     // 全局配置
	"go-mysql-transfer/util/etcds" // etcd工具函数
)

// etcdPositionStorage etcd位置存储实现
// 实现了PositionStorage接口，使用etcd作为存储后端
// 适用于集群部署模式，多个节点可以共享同一个位置信息
// etcd提供强一致性和高可用性保证
type etcdPositionStorage struct {
	// 空结构体，所有操作通过全局etcd连接进行
}

// Initialize 初始化etcd位置存储
// 在etcd中创建位置信息键（如果不存在）
// 使用JSON格式存储空的位置信息作为初始值
// 返回初始化过程中的错误
func (s *etcdPositionStorage) Initialize() error {
	// 创建空的位置记录并序列化为JSON
	data, err := json.Marshal(mysql.Position{})
	if err != nil {
		return err // JSON序列化失败
	}

	// 在etcd中创建位置信息键（如果不存在）
	// 使用ZkPositionDir()作为键名（虽然名字带zk，但在etcd中复用）
	err = etcds.CreateIfNecessary(global.Cfg().ZkPositionDir(), string(data), _etcdOps)
	if err != nil {
		return err // etcd操作失败
	}

	return nil
}

// Save 保存binlog位置信息到etcd
// pos: 要保存的MySQL binlog位置信息
// 使用JSON格式序列化位置信息，然后存储到etcd
// 返回保存过程中的错误
func (s *etcdPositionStorage) Save(pos mysql.Position) error {
	// 将位置信息序列化为JSON格式
	data, err := json.Marshal(pos)
	if err != nil {
		return err // JSON序列化失败
	}

	// 保存到etcd，覆盖现有值
	return etcds.Save(global.Cfg().ZkPositionDir(), string(data), _etcdOps)
}

// Get 从etcd获取当前保存的binlog位置信息
// 返回位置信息和可能的错误
// 从etcd读取JSON数据并反序列化为Position结构
func (s *etcdPositionStorage) Get() (mysql.Position, error) {
	var entity mysql.Position // 用于存储反序列化后的位置信息

	// 从etcd获取位置数据
	// 返回值包括：数据内容、版本信息、错误
	data, _, err := etcds.Get(global.Cfg().ZkPositionDir(), _etcdOps)
	if err != nil {
		return entity, err // etcd读取失败
	}

	// 将JSON数据反序列化为Position结构
	err = json.Unmarshal(data, &entity)

	return entity, err
}
