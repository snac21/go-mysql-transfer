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

// Package election 分布式选举服务模块
// 提供基于ZooKeeper和etcd的分布式选举功能，确保集群中只有一个主节点执行数据同步任务
// 支持主节点故障自动切换，保证服务的高可用性
package election

import (
	"go-mysql-transfer/global" // 全局配置管理
)

// Service 选举服务接口
// 定义了分布式选举的标准操作，支持多种分布式协调服务实现
type Service interface {
	// Elect 启动选举过程
	// 参与集群选举，尝试成为主节点或确认当前主节点
	// 返回选举过程中可能出现的错误
	Elect() error

	// IsLeader 判断当前节点是否为主节点
	// 返回true表示当前节点是主节点，false表示是从节点
	IsLeader() bool

	// Leader 获取当前主节点标识
	// 返回当前集群中主节点的标识字符串
	Leader() string

	// Nodes 获取集群中所有节点列表
	// 返回参与选举的所有节点标识列表
	Nodes() []string
}

// NewElection 创建选举服务实例
// 根据全局配置选择相应的分布式协调服务实现
// _informCh: 选举结果通知通道，用于向上层服务通知选举状态变化
// 返回对应的选举服务实现实例
func NewElection(_informCh chan bool) Service {
	if global.Cfg().IsZk() {
		// 使用ZooKeeper作为分布式协调服务
		return newZkElection(_informCh)
	} else {
		// 使用etcd作为分布式协调服务
		return newEtcdElection(_informCh)
	}
}
