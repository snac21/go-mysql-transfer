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

// Package storage 选举存储模块
// 提供分布式选举功能，用于集群模式下的主节点选举
// 确保在多节点部署时只有一个节点作为主节点处理数据同步
// 注意：此模块当前处于开发阶段，部分功能尚未完整实现
package storage

import "go-mysql-transfer/global" // 全局配置

// ElectionStorage 分布式选举存储接口
// 定义了集群环境下主节点选举的标准方法
// 用于确保在多节点部署时的数据同步一致性
type ElectionStorage interface {
	// Elect 执行选举操作
	// 尝试成为集群中的主节点（Leader）
	// 返回选举过程中的错误
	// 成功返回nil表示当前节点成为主节点
	// 失败表示其他节点已经是主节点或选举过程出错
	Elect() error
}

// NewElectionStorage 选举存储工厂方法
// 根据配置创建相应的选举存储实现
// conf: 全局配置对象
// 返回具体的选举存储实现实例
// 注意：当前实现不完整，etcd分支未实现，返回类型也不正确
func NewElectionStorage(conf *global.Config) PositionStorage {
	// 如果配置了集群模式
	if conf.IsCluster() {
		// 使用ZooKeeper进行分布式选举
		if conf.IsZk() {
			// 注意：这里返回类型不正确，应该返回ElectionStorage而不是PositionStorage
			// 且应该有专门的zkElectionStorage实现
			return &zkPositionStorage{}
		}
		// 使用etcd进行分布式选举
		if conf.IsEtcd() {
			// TODO: 实现etcd选举存储
			// 应该返回&etcdElectionStorage{}
		}
	}

	// 单机模式不需要选举
	return nil
}
