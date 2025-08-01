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

// Package service 提供核心业务服务的初始化和管理功能
// 主要负责数据传输服务、集群服务和选举服务的生命周期管理
// 支持单机模式和集群模式两种部署方式
package service

import (
	"go-mysql-transfer/global"           // 全局配置管理
	"go-mysql-transfer/service/election" // 集群选举服务
)

// 全局服务实例变量
// 使用包级别变量实现单例模式，确保服务实例的唯一性
var (
	_transferService *TransferService // 数据传输服务实例，负责MySQL binlog监听和数据同步
	_electionService election.Service // 选举服务实例，用于集群模式下的主节点选举
	_clusterService  *ClusterService  // 集群服务实例，管理集群状态和节点协调
)

// Initialize 初始化所有核心服务
// 根据配置决定是否启用集群模式，并初始化相应的服务组件
// 返回初始化过程中可能出现的错误
func Initialize() error {
	// 创建数据传输服务实例
	transferService := &TransferService{
		loopStopSignal: make(chan struct{}, 1), // 创建停止信号通道，容量为1避免阻塞
	}

	// 初始化传输服务，包括Canal配置、规则加载、端点连接等
	err := transferService.initialize()
	if err != nil {
		return err // 初始化失败时返回错误
	}
	_transferService = transferService // 保存服务实例到全局变量

	// 检查是否启用集群模式
	if global.Cfg().IsCluster() {
		// 创建集群服务实例
		_clusterService = &ClusterService{
			electionSignal: make(chan bool, 1), // 创建选举信号通道，用于接收选举结果
		}

		// 创建选举服务实例，传入选举信号通道
		// 选举服务负责在多个节点中选出主节点来执行数据同步任务
		_electionService = election.NewElection(_clusterService.electionSignal)
	}

	return nil // 初始化成功
}

// StartUp 启动核心服务
// 根据配置的运行模式（单机或集群）启动相应的服务
// 集群模式：启动集群服务，由选举决定是否执行数据同步
// 单机模式：直接启动数据传输服务
func StartUp() {
	if global.Cfg().IsCluster() {
		// 集群模式：启动集群服务
		// 集群服务会参与选举，只有被选为主节点的实例才会执行数据同步
		_clusterService.boot()
	} else {
		// 单机模式：直接启动数据传输服务
		// 立即开始监听MySQL binlog并执行数据同步
		_transferService.StartUp()
	}
}

// Close 关闭所有服务
// 优雅关闭数据传输服务，确保正在处理的数据完成同步
// 注意：这里只关闭传输服务，集群服务和选举服务的关闭由各自管理
func Close() {
	_transferService.Close() // 关闭数据传输服务，停止binlog监听和数据处理
}

// TransferServiceIns 获取数据传输服务实例
// 提供全局访问数据传输服务的入口，用于外部模块调用
// 返回当前的传输服务实例，如果未初始化则返回nil
func TransferServiceIns() *TransferService {
	return _transferService
}

// ClusterServiceIns 获取集群服务实例
// 提供全局访问集群服务的入口，用于集群状态查询和管理
// 返回当前的集群服务实例，如果未启用集群模式则返回nil
func ClusterServiceIns() *ClusterService {
	return _clusterService
}
