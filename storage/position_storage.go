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

// Package storage 位置存储接口定义模块
// 定义了MySQL binlog位置信息存储的统一接口
// 支持多种存储后端实现，提供工厂方法创建具体实现
package storage

import (
	"github.com/go-mysql-org/go-mysql/mysql" // MySQL相关类型定义

	"go-mysql-transfer/global" // 全局配置
)

// PositionStorage MySQL binlog位置存储接口
// 定义了存储和获取MySQL binlog位置信息的标准方法
// binlog位置信息用于记录数据同步的进度，确保断点续传和数据一致性
type PositionStorage interface {
	// Initialize 初始化存储
	// 创建必要的存储结构（如数据库表、文件目录等）
	// 返回初始化过程中的错误
	Initialize() error

	// Save 保存binlog位置信息
	// pos: MySQL binlog位置信息，包含文件名和偏移量
	// 返回保存过程中的错误
	Save(pos mysql.Position) error

	// Get 获取当前保存的binlog位置信息
	// 返回位置信息和可能的错误
	// 用于应用启动时恢复同步进度
	Get() (mysql.Position, error)
}

// NewPositionStorage 位置存储工厂方法
// 根据全局配置创建相应的位置存储实现
// 支持的存储后端：
// 1. 集群模式：ZooKeeper或etcd（用于多节点共享位置信息）
// 2. 单机模式：BoltDB（本地嵌入式数据库）
// 返回具体的位置存储实现实例
func NewPositionStorage() PositionStorage {
	// 如果配置了集群模式
	if global.Cfg().IsCluster() {
		// 使用ZooKeeper作为分布式存储
		if global.Cfg().IsZk() {
			return &zkPositionStorage{}
		}
		// 使用etcd作为分布式存储
		if global.Cfg().IsEtcd() {
			return &etcdPositionStorage{}
		}
	}

	// 默认使用BoltDB本地存储
	return &boltPositionStorage{}
}
