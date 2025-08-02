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

// Package service 集群服务模块
// 提供集群模式下的主节点选举、状态管理和服务协调功能
// 确保在多节点部署时只有一个节点执行数据同步任务，避免数据重复处理
package service

import (
	// 标准日志

	"go-mysql-transfer/global"  // 全局配置和状态管理
	"go-mysql-transfer/metrics" // 监控指标
	"go-mysql-transfer/util/logs"
)

// ClusterService 集群服务结构体
// 负责管理集群状态、处理选举结果、协调节点间的工作分配
type ClusterService struct {
	electionSignal chan bool // 选举信号通道，接收选举服务发送的选举结果
	// true: 当前节点被选为主节点
	// false: 当前节点为从节点
}

// boot 启动集群服务
// 初始化选举过程并启动选举结果监听器
// 返回启动过程中可能出现的错误
func (s *ClusterService) boot() error {
	logs.Info("start master election") // 记录选举开始

	// 启动主节点选举过程
	// 选举服务会与其他节点协商，确定哪个节点作为主节点
	err := _electionService.Elect()
	if err != nil {
		return err // 选举启动失败
	}

	// 启动选举结果监听器
	// 监听选举状态变化，根据选举结果启动或停止数据同步服务
	s.startElectListener()

	return nil // 启动成功
}

// startElectListener 启动选举结果监听器
// 在独立的goroutine中运行，持续监听选举信号并根据结果调整服务状态
func (s *ClusterService) startElectListener() {
	go func() {
		// 持续监听选举信号
		// 当选举状态发生变化时，会收到相应的信号
		for {
			select {
			case selected := <-s.electionSignal:
				// 收到选举结果信号
				logs.Infof("Election result received: selected=%v", selected)

				// 1. 更新全局状态
				// 设置当前的主节点信息和本节点的角色标志
				global.SetLeaderNode(_electionService.Leader()) // 设置主节点标识
				global.SetLeaderFlag(selected)                  // 设置本节点是否为主节点

				// 2. 根据选举结果调整服务状态
				if selected {
					// 当前节点被选为主节点
					logs.Info("This node is elected as leader, starting transfer service")

					// 更新监控指标为主节点状态
					metrics.SetLeaderState(metrics.LeaderState)

					// 启动数据传输服务，开始执行数据同步任务
					_transferService.StartUp()
				} else {
					// 当前节点为从节点
					logs.Info("This node is follower, stopping transfer service")

					// 更新监控指标为从节点状态
					metrics.SetLeaderState(metrics.FollowerState)

					// 停止数据传输服务，避免重复处理数据
					// 只有主节点才执行数据同步，从节点保持待机状态
					_transferService.stopDump()
				}
			}
		}
	}()
}

// Nodes 获取集群中所有节点列表
// 返回当前集群中参与选举的所有节点标识
// 用于集群状态监控和管理
func (s *ClusterService) Nodes() []string {
	return _electionService.Nodes() // 委托给选举服务获取节点列表
}
