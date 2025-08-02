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

// Package election 基于ZooKeeper的分布式选举实现
// 利用ZooKeeper的临时节点和监听机制实现主节点选举
// 支持网络分区检测和自动降级机制
package election

import (
	// 格式化输出
	"sync" // 同步原语

	"github.com/samuel/go-zookeeper/zk" // ZooKeeper客户端
	"go.uber.org/atomic"                // 原子操作

	"go-mysql-transfer/global"    // 全局配置
	"go-mysql-transfer/storage"   // 存储层
	"go-mysql-transfer/util/logs" // 日志工具
)

// zkElection 基于ZooKeeper的选举服务实现
// 使用ZooKeeper的临时节点机制实现主节点选举和故障检测
type zkElection struct {
	once     sync.Once // 确保某些初始化操作只执行一次
	informCh chan bool // 选举结果通知通道，向上层服务通知选举状态变化

	// 原子变量，保证并发安全
	selected atomic.Bool   // 当前节点是否被选为主节点
	leader   atomic.String // 当前主节点标识

	// 网络分区检测相关
	connectingAmount atomic.Int64 // 连接中状态的计数器，用于检测网络分区
	downgraded       atomic.Bool  // 是否已降级为从节点（网络分区时）
}

// newZkElection 创建基于ZooKeeper的选举服务实例
// _informCh: 选举结果通知通道
// 返回配置完成的ZooKeeper选举服务实例
func newZkElection(_informCh chan bool) *zkElection {
	return &zkElection{
		informCh: _informCh, // 设置选举结果通知通道
	}
}

// Elect 启动选举过程
// 使用ZooKeeper的临时节点机制实现选举
// 尝试创建选举节点，成功则成为主节点，失败则成为从节点
// 返回选举过程中可能出现的错误
func (s *zkElection) Elect() error {
	// 1. 准备节点数据和访问控制
	data := []byte(global.CurrentNode()) // 当前节点标识作为节点数据
	acl := zk.WorldACL(zk.PermAll)       // 设置访问权限为所有人可访问

	// 2. 尝试创建选举节点
	// 使用临时节点，当连接断开时节点会自动删除
	_, err := storage.ZKConn().Create(global.Cfg().ZkElectionDir(), data, zk.FlagEphemeral, acl)
	if err == nil {
		// 创建成功，当前节点成为主节点
		s.beLeader()
	} else {
		// 创建失败，说明已有主节点存在
		// 3. 获取当前主节点信息
		v, _, err := storage.ZKConn().Get(global.Cfg().ZkElectionDir())
		if err != nil {
			return err // 获取主节点信息失败
		}

		leader := string(v) // 主节点标识

		// 4. 判断是否需要重新成为主节点
		// 如果当前节点之前是主节点但因网络问题降级，现在网络恢复了
		if leader == global.CurrentNode() && s.downgraded.Load() {
			s.beLeader() // 重新成为主节点
		} else {
			s.beFollower(leader) // 成为从节点
		}
	}

	// 5. 注册当前节点到节点列表
	// 在节点目录下创建当前节点的临时节点，用于节点发现
	dir := global.Cfg().ZkNodesDir() + "/" + global.CurrentNode()
	storage.ZKConn().Create(dir, data, zk.FlagEphemeral, acl)

	// 6. 启动监控任务（只启动一次）
	s.once.Do(func() {
		s.startConnectionWatchTask() // 启动连接状态监控
		s.startNodeWatchTask()       // 启动节点变化监控
	})

	return nil // 选举过程完成
}

// IsLeader 判断当前节点是否为主节点
// 返回当前节点的选举状态
// true: 当前节点是主节点，false: 当前节点是从节点
func (s *zkElection) IsLeader() bool {
	return s.selected.Load() // 原子读取选举状态
}

// Leader 获取当前主节点标识
// 返回当前集群中主节点的标识字符串
// 如果没有主节点或主节点未知，返回空字符串
func (s *zkElection) Leader() string {
	return s.leader.Load() // 原子读取主节点标识
}

// Nodes 获取集群中所有参与选举的节点列表
// 从ZooKeeper中查询所有注册的节点
// 返回节点标识字符串列表，如果查询失败返回nil
func (s *zkElection) Nodes() []string {
	// 获取节点目录下的所有子节点
	v, _, err := storage.ZKConn().Children(global.Cfg().ZkNodesDir())
	if err != nil {
		return nil // 查询失败返回nil
	}
	return v // 返回节点列表
}

// beLeader 设置当前节点为主节点状态
// 更新内部状态并通知上层服务
func (s *zkElection) beLeader() {
	s.selected.Store(true)                      // 原子设置为主节点状态
	s.leader.Store(global.CurrentNode())        // 设置主节点标识为当前节点
	s.informCh <- s.selected.Load()             // 通知上层服务选举结果
	logs.Info("the current node is the master") // 记录主节点状态
}

// beFollower 设置当前节点为从节点状态
// leader: 当前主节点的标识，可以为空字符串表示主节点未知
func (s *zkElection) beFollower(leader string) {
	s.selected.Store(false)         // 原子设置为从节点状态
	s.leader.Store(leader)          // 设置主节点标识
	s.informCh <- s.selected.Load() // 通知上层服务选举结果

	// 记录从节点状态和主节点信息
	logs.Infof("The current node is the follower, master node is: %s", leader)
}

// startConnectionWatchTask 启动ZooKeeper连接状态监控任务
// 监控ZooKeeper连接状态变化，实现网络分区检测和自动恢复
func (s *zkElection) startConnectionWatchTask() {
	logs.Info("Start zookeeper connection Status watch task")

	go func() {
		// 持续监听ZooKeeper连接状态变化
		for event := range storage.ZKStatusSignal() {
			logs.Infof("ZK ConnStatus: %v", event)

			// 1. 处理主节点的连接状态变化
			if s.selected.Load() {
				// 如果当前是主节点且连接状态为连接中
				if zk.StateConnecting == event.State {
					s.connectingAmount.Inc() // 增加连接中计数
				}

				// 2. 网络分区检测
				// 如果连接中状态次数超过ZooKeeper服务器数量，可能发生了网络分区
				if s.connectingAmount.Load() > int64(len(storage.ZKAddresses())) {
					s.downgrading() // 主动降级为从节点
				}
			}

			// 3. 处理会话恢复
			if zk.StateHasSession == event.State {
				s.connectingAmount.Store(0) // 重置连接中计数

				// 如果之前因网络分区降级，现在网络恢复了，重新参与选举
				if s.downgraded.Load() {
					logs.Info("zookeeper HasSession restart elect")
					s.Elect()                 // 重新参与选举
					s.downgraded.Store(false) // 清除降级标志
				}
			}
		}
	}()
}

// startNodeWatchTask 启动选举节点监控任务
// 监控选举节点的变化，当主节点失效时自动触发重新选举
func (s *zkElection) startNodeWatchTask() {
	go func() {
		logs.Info("Start zookeeper election node watch task")

		// 监听选举节点的子节点变化
		_, _, ch, _ := storage.ZKConn().ChildrenW(global.Cfg().ZkElectionDir())

		// 持续监听节点变化事件
		for childEvent := range ch {
			// 当选举节点被删除时（主节点失效）
			if childEvent.Type == zk.EventNodeDeleted {
				logs.Info("Start elect new master ...")

				// 触发重新选举
				err := s.Elect()
				if err != nil {
					logs.Errorf("elect new master error %s ", err.Error())
				}
			}
		}
	}()
}

// downgrading 执行主节点降级操作
// 当检测到网络分区时，主动将当前主节点降级为从节点
// 这是一种防脑裂机制，避免网络分区时出现多个主节点
func (s *zkElection) downgrading() {
	// 使用原子操作检查并设置降级状态，避免重复降级
	if !s.downgraded.Load() {
		logs.Warn("Lost contact with zookeeper, The current node degraded to Follower")
		s.downgraded.Store(true) // 设置降级标志
		s.beFollower("")         // 降级为从节点，主节点未知
	}
}
