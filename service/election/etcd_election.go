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

// Package election 基于etcd的分布式选举实现
// 利用etcd的分布式锁和会话机制实现主节点选举
// 支持自动故障检测和主节点切换
package election

import (
	"context" // 上下文管理
	// 格式化输出
	"log"  // 标准日志
	"sync" // 同步原语
	"time" // 时间处理

	"github.com/juju/errors"                // 错误处理增强
	"go.etcd.io/etcd/client/v3/concurrency" // etcd并发控制
	"go.uber.org/atomic"                    // 原子操作

	"go-mysql-transfer/global"     // 全局配置
	"go-mysql-transfer/storage"    // 存储层
	"go-mysql-transfer/util/etcds" // etcd工具
	"go-mysql-transfer/util/logs"  // 日志工具
)

// _electionNodeTTL 选举节点的TTL（生存时间），单位：秒
// 用于etcd会话的租约时间，节点必须在此时间内续约以保持活跃状态
const _electionNodeTTL = 2

// etcdElection 基于etcd的选举服务实现
// 使用etcd的分布式锁机制实现主节点选举和故障切换
type etcdElection struct {
	once sync.Once // 确保某些初始化操作只执行一次

	informCh chan bool // 选举结果通知通道，向上层服务通知选举状态变化

	// 原子变量，保证并发安全
	selected atomic.Bool   // 当前节点是否被选为主节点
	ensured  atomic.Bool   // 是否已确保从节点状态
	leader   atomic.String // 当前主节点标识
}

// newEtcdElection 创建基于etcd的选举服务实例
// _informCh: 选举结果通知通道
// 返回配置完成的etcd选举服务实例
func newEtcdElection(_informCh chan bool) *etcdElection {
	return &etcdElection{
		informCh: _informCh, // 设置选举结果通知通道
	}
}

// Elect 启动选举过程
// 同时启动主节点选举和从节点状态确保两个并发任务
// 返回选举启动过程中的错误（通常为nil，因为选举在后台异步进行）
func (s *etcdElection) Elect() error {
	s.doElect()        // 启动主节点选举流程
	s.ensureFollower() // 启动从节点状态确保流程
	return nil         // 选举过程异步进行，立即返回成功
}

// doElect 执行主节点选举的核心逻辑
// 在独立的goroutine中运行，持续参与选举过程
// 使用etcd的分布式锁机制实现选举
func (s *etcdElection) doElect() {
	go func() {
		// 持续选举循环，确保在主节点失效时能够重新选举
		for {
			// 1. 创建etcd会话
			// 会话用于维持与etcd的连接，并提供租约机制
			session, err := concurrency.NewSession(storage.EtcdConn(), concurrency.WithTTL(_electionNodeTTL))
			if err != nil {
				logs.Error(err.Error())
				return // 会话创建失败，退出选举
			}

			// 2. 创建选举对象
			// 基于etcd的分布式锁实现选举机制
			elc := concurrency.NewElection(session, global.Cfg().ZkElectionDir())
			ctx := context.Background()

			// 3. 参与选举竞选
			// Campaign方法会尝试获取分布式锁，成功则成为主节点
			if err = elc.Campaign(ctx, global.CurrentNode()); err != nil {
				logs.Error(errors.ErrorStack(err))
				session.Close()  // 关闭会话
				s.beFollower("") // 设置为从节点状态
				continue         // 继续下一轮选举
			}

			// 4. 检查选举结果
			select {
			case <-session.Done():
				// 会话已结束，可能是网络问题或etcd故障
				s.beFollower("") // 设置为从节点状态
				continue         // 重新开始选举
			default:
				// 选举成功，成为主节点
				s.beLeader()

				// 5. 记录选举结果到etcd
				// 将选举信息写入etcd，供其他节点查询
				err = etcds.UpdateOrCreate(global.Cfg().ZkElectedDir(), elc.Key(), storage.EtcdOps())
				if err != nil {
					logs.Error(errors.ErrorStack(err))
					return // 记录失败，退出选举
				}
			}

			// 6. 主节点状态维持循环
			// 作为主节点期间，需要持续监控会话状态
			shouldBreak := false
			for !shouldBreak {
				select {
				case <-session.Done():
					// etcd会话结束，失去主节点资格
					logs.Warn("etcd session has done")
					shouldBreak = true
					s.beFollower("") // 降级为从节点
					break            // 退出维持循环，重新参与选举
				case <-ctx.Done():
					// 上下文取消，主动放弃主节点资格
					ctxTmp, cancel := context.WithTimeout(context.Background(), time.Second*_electionNodeTTL)
					defer cancel()     // 确保取消函数被调用，避免上下文泄露
					elc.Resign(ctxTmp) // 主动辞去主节点职务
					session.Close()    // 关闭会话
					s.beFollower("")   // 设置为从节点状态
					return             // 退出选举流程
				}
			}
		}
	}()
}

// IsLeader 判断当前节点是否为主节点
// 返回当前节点的选举状态
// true: 当前节点是主节点，false: 当前节点是从节点
func (s *etcdElection) IsLeader() bool {
	return s.selected.Load() // 原子读取选举状态
}

// Leader 获取当前主节点标识
// 返回当前集群中主节点的标识字符串
// 如果没有主节点或主节点未知，返回空字符串
func (s *etcdElection) Leader() string {
	return s.leader.Load() // 原子读取主节点标识
}

// ensureFollower 确保从节点状态的正确性
// 在独立的goroutine中运行，用于非主节点确认当前主节点信息
// 这个方法确保从节点能够正确识别当前的主节点
func (s *etcdElection) ensureFollower() {
	go func() {
		// 持续检查直到当前节点成为主节点或确认了主节点信息
		for {
			// 如果当前节点已经是主节点，则无需继续检查
			if s.selected.Load() {
				break // 退出检查循环
			}

			// 1. 获取选举结果信息
			// 从etcd中读取当前选举的结果信息
			k, _, err := etcds.Get(global.Cfg().ZkElectedDir(), storage.EtcdOps())
			if err != nil {
				logs.Error(errors.ErrorStack(err))
				continue // 获取失败，继续重试
			}

			// 2. 获取主节点详细信息
			// 根据选举结果键获取主节点的具体信息
			var l []byte
			l, _, err = etcds.Get(string(k), storage.EtcdOps())
			if err != nil {
				logs.Error(errors.ErrorStack(err))
				continue // 获取失败，继续重试
			}

			// 3. 确认从节点状态
			s.ensured.Store(true)   // 标记已确保从节点状态
			s.beFollower(string(l)) // 设置为从节点，并记录主节点信息
			break                   // 状态确认完成，退出循环
		}
	}()
}

// Nodes 获取集群中所有参与选举的节点列表
// 从etcd中查询所有注册的选举节点
// 返回节点标识字符串列表，如果查询失败返回空列表
func (s *etcdElection) Nodes() []string {
	var nodes []string

	// 从etcd中列出所有选举节点
	// 注意：这里使用硬编码路径，在实际使用中应该使用配置
	ls, err := etcds.List("/transfer/myTransfer/election", storage.EtcdOps())
	if err == nil {
		// 遍历所有节点，提取节点标识
		for _, v := range ls {
			nodes = append(nodes, string(v.Value))
		}
	}
	return nodes
}

// beLeader 设置当前节点为主节点状态
// 更新内部状态并通知上层服务
func (s *etcdElection) beLeader() {
	s.selected.Store(true)                        // 原子设置为主节点状态
	s.leader.Store(global.CurrentNode())          // 设置主节点标识为当前节点
	s.informCh <- s.selected.Load()               // 通知上层服务选举结果
	log.Println("the current node is the master") // 记录主节点状态
}

// beFollower 设置当前节点为从节点状态
// leader: 当前主节点的标识，可以为空字符串表示主节点未知
func (s *etcdElection) beFollower(leader string) {
	s.selected.Store(false)         // 原子设置为从节点状态
	s.informCh <- s.selected.Load() // 通知上层服务选举结果
	s.leader.Store(leader)          // 设置主节点标识

	// 记录从节点状态和主节点信息
	log.Printf("The current node is the follower, master node is : %s", s.leader.Load())
}
