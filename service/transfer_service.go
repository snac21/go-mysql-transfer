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

// Package service 数据传输服务模块
// 提供MySQL binlog监听、数据解析、规则匹配、目标端点数据传输等核心功能
// 支持多种目标系统：Redis、MongoDB、Elasticsearch、Kafka、RabbitMQ、RocketMQ等
package service

import (
	"fmt"    // 格式化输出
	"regexp" // 正则表达式处理
	"sync"   // 同步原语
	"time"   // 时间处理

	"github.com/go-mysql-org/go-mysql/canal" // MySQL binlog解析库
	"github.com/go-mysql-org/go-mysql/mysql" // MySQL协议和类型定义
	"github.com/juju/errors"                 // 错误处理增强
	"go.uber.org/atomic"                     // 原子操作

	"go-mysql-transfer/global"           // 全局配置和规则管理
	"go-mysql-transfer/metrics"          // 监控指标
	"go-mysql-transfer/service/endpoint" // 目标端点抽象
	"go-mysql-transfer/storage"          // 存储层抽象
	"go-mysql-transfer/util/logs"        // 日志工具
)

// _transferLoopInterval 传输服务监控循环间隔（秒）
// 用于定期检查目标端点的连接状态，如果连接断开会尝试重连
const _transferLoopInterval = 1

// TransferService 数据传输服务结构体
// 负责MySQL binlog监听、数据解析、规则匹配和目标端点数据传输的完整流程
type TransferService struct {
	// Canal相关组件 - 负责MySQL binlog监听和解析
	canal        *canal.Canal  // Canal实例，用于连接MySQL并监听binlog
	canalCfg     *canal.Config // Canal配置，包含MySQL连接信息和过滤规则
	canalHandler *handler      // 自定义事件处理器，处理binlog事件
	canalEnable  atomic.Bool   // Canal启用状态标志，原子操作确保线程安全
	lockOfCanal  sync.Mutex    // Canal操作锁，防止并发操作导致的状态不一致
	firstsStart  atomic.Bool   // 首次启动标志，用于区分首次启动和重启

	// 并发控制和端点管理
	wg             sync.WaitGroup          // 等待组，用于优雅关闭时等待所有goroutine完成
	endpoint       endpoint.Endpoint       // 目标端点接口，抽象了不同目标系统的操作
	endpointEnable atomic.Bool             // 端点启用状态标志，用于控制数据传输
	positionDao    storage.PositionStorage // 位置存储DAO，用于保存和恢复binlog位置
	loopStopSignal chan struct{}           // 监控循环停止信号通道
}

// initialize 初始化传输服务的所有组件
// 包括Canal配置、规则处理、位置存储、目标端点等核心组件的初始化
// 返回初始化过程中可能出现的错误
func (s *TransferService) initialize() error {
	// 1. 初始化Canal配置
	// 使用默认配置作为基础，然后根据全局配置进行定制
	s.canalCfg = canal.NewDefaultConfig()
	s.canalCfg.Addr = global.Cfg().Addr                          // MySQL服务器地址（host:port）
	s.canalCfg.User = global.Cfg().User                          // MySQL用户名
	s.canalCfg.Password = global.Cfg().Password                  // MySQL密码
	s.canalCfg.Charset = global.Cfg().Charset                    // 字符集设置
	s.canalCfg.Flavor = global.Cfg().Flavor                      // MySQL版本类型（mysql/mariadb）
	s.canalCfg.ServerID = global.Cfg().SlaveID                   // 从服务器ID，必须唯一
	s.canalCfg.Dump.ExecutionPath = global.Cfg().DumpExec        // mysqldump可执行文件路径
	s.canalCfg.Dump.DiscardErr = false                           // 不忽略dump错误，确保数据完整性
	s.canalCfg.Dump.SkipMasterData = global.Cfg().SkipMasterData // 是否跳过主数据信息

	// 2. 创建Canal实例
	// Canal是MySQL binlog监听的核心组件
	if err := s.createCanal(); err != nil {
		return errors.Trace(err) // 保留错误堆栈信息
	}

	// 3. 完善同步规则
	// 处理通配符规则、验证表结构、编译Lua脚本等
	if err := s.completeRules(); err != nil {
		return errors.Trace(err)
	}

	// 4. 添加需要监听的数据库和表
	// 根据规则配置设置Canal的过滤条件
	s.addDumpDatabaseOrTable()

	// 5. 初始化位置存储DAO
	// 用于保存和恢复binlog同步位置，支持断点续传
	positionDao := storage.NewPositionStorage()
	if err := positionDao.Initialize(); err != nil {
		return errors.Trace(err)
	}
	s.positionDao = positionDao

	// 6. 初始化目标端点
	// 根据配置创建对应的目标系统连接（Redis、MongoDB、ES等）
	endpoint := endpoint.NewEndpoint(s.canal)
	if err := endpoint.Connect(); err != nil {
		return errors.Trace(err)
	}

	// 7. 验证端点连接状态
	// 对于MongoDB等异步连接的系统，需要主动ping确认连接成功
	if global.Cfg().IsMongodb() {
		err := endpoint.Ping()
		if err != nil {
			return err // MongoDB连接验证失败
		}
	}

	// 8. 设置端点状态和监控指标
	s.endpoint = endpoint
	s.endpointEnable.Store(true)              // 标记端点为可用状态
	metrics.SetDestState(metrics.DestStateOK) // 更新监控指标

	// 9. 设置首次启动标志并启动监控循环
	s.firstsStart.Store(true) // 标记为首次启动
	s.startLoop()             // 启动端点状态监控循环

	return nil // 初始化成功
}

// run 启动Canal binlog监听
// 从指定位置开始监听MySQL binlog，并在独立的goroutine中运行
// 返回启动过程中可能出现的错误
func (s *TransferService) run() error {
	// 1. 获取当前同步位置
	// 从位置存储中获取上次同步的binlog位置，用于断点续传
	current, err := s.positionDao.Get()
	if err != nil {
		return err // 获取位置失败，无法启动同步
	}

	// 2. 启动Canal监听goroutine
	// 使用WaitGroup确保优雅关闭时能等待goroutine完成
	s.wg.Add(1)
	go func(p mysql.Position) {
		// 在goroutine结束时执行清理工作
		defer func() {
			logs.Info("Canal monitoring goroutine is exiting")
			s.canalEnable.Store(false) // 标记Canal为禁用状态
			s.canal = nil              // 清空Canal实例引用
			s.wg.Done()                // 通知WaitGroup当前goroutine已完成
		}()

		// 3. 启动Canal监听
		s.canalEnable.Store(true) // 标记Canal为启用状态
		logs.Infof("transfer run from position(%s %d)", p.Name, p.Pos)

		// 从指定位置开始监听binlog
		// RunFrom是阻塞调用，会持续监听直到出错或被关闭
		if err := s.canal.RunFrom(p); err != nil {
			// Canal运行出错，记录错误信息
			logs.Errorf("start transfer: %v", err)
			logs.Errorf("canal: %v", errors.ErrorStack(err))

			// 4. 错误处理：停止事件监听器
			if s.canalHandler != nil {
				s.canalHandler.stopListener() // 停止事件处理器
			}
			s.canalEnable.Store(false) // 标记Canal为禁用状态
		}

		logs.Info("Canal is Closed") // Canal正常关闭
	}(current) // 将当前位置作为参数传递给goroutine

	// 5. 等待Canal启动完成
	// Canal的RunFrom方法没有提供启动完成的回调
	// 这里等待1秒确保Canal已经成功启动并开始监听
	time.Sleep(time.Second)

	return nil // 启动成功
}

// StartUp 启动数据传输服务
// 根据是否首次启动选择不同的启动策略
// 首次启动：创建新的事件处理器并启动监听
// 重新启动：先清理现有资源，然后重新创建和启动
func (s *TransferService) StartUp() {
	// 使用互斥锁确保启动过程的原子性
	// 防止并发调用导致的状态不一致
	s.lockOfCanal.Lock()
	defer s.lockOfCanal.Unlock()

	// 检查是否为首次启动
	if s.firstsStart.Load() {
		// 首次启动流程
		logs.Info("Starting transfer service for the first time")

		// 1. 创建事件处理器
		// 处理器负责接收和处理Canal产生的binlog事件
		s.canalHandler = newHandler()

		// 2. 设置Canal的事件处理器
		// 将自定义处理器注册到Canal，接收binlog事件
		s.canal.SetEventHandler(s.canalHandler)

		// 3. 启动事件监听器
		// 在独立的goroutine中启动事件处理循环
		s.canalHandler.startListener()

		// 4. 标记非首次启动
		s.firstsStart.Store(false)

		// 5. 启动Canal监听
		s.run()

		logs.Info("Transfer service started successfully")
	} else {
		// 重新启动流程
		logs.Info("Restarting transfer service")
		s.restart()
	}
}

// restart 重新启动数据传输服务
// 用于服务异常后的恢复，会先清理现有资源再重新初始化
func (s *TransferService) restart() {
	logs.Info("Restarting transfer service...")

	// 1. 清理现有Canal资源
	if s.canal != nil {
		logs.Info("Closing existing canal connection")
		s.canal.Close() // 关闭Canal连接
		s.wg.Wait()     // 等待所有相关goroutine完成
		logs.Info("Canal connection closed")
	}

	// 2. 重新创建Canal实例
	// 使用现有配置重新创建Canal，恢复MySQL连接
	logs.Info("Recreating canal instance")
	s.createCanal()

	// 3. 重新设置监听的数据库和表
	// 根据规则配置重新设置过滤条件
	s.addDumpDatabaseOrTable()

	// 4. 创建新的事件处理器
	// 重新创建处理器确保状态清洁
	s.canalHandler = newHandler()

	// 5. 注册事件处理器到Canal
	s.canal.SetEventHandler(s.canalHandler)

	// 6. 启动事件监听器
	s.canalHandler.startListener()

	// 7. 启动Canal监听
	s.run()

	logs.Info("Transfer service restarted successfully")
}

func (s *TransferService) stopDump() {
	s.lockOfCanal.Lock()
	defer s.lockOfCanal.Unlock()

	if s.canal == nil {
		return
	}

	if !s.canalEnable.Load() {
		return
	}

	if s.canalHandler != nil {
		s.canalHandler.stopListener()
		s.canalHandler = nil
	}

	s.canal.Close()
	s.wg.Wait()

	logs.Info("dumper stopped")
}

func (s *TransferService) Close() {
	s.stopDump()
	s.loopStopSignal <- struct{}{}
}

func (s *TransferService) Position() (mysql.Position, error) {
	return s.positionDao.Get()
}

func (s *TransferService) createCanal() error {
	for _, rc := range global.Cfg().RuleConfigs {
		s.canalCfg.IncludeTableRegex = append(s.canalCfg.IncludeTableRegex, rc.Schema+"\\."+rc.Table)
	}
	var err error
	s.canal, err = canal.NewCanal(s.canalCfg)
	return errors.Trace(err)
}

func (s *TransferService) completeRules() error {
	wildcards := make(map[string]bool)
	for _, rc := range global.Cfg().RuleConfigs {
		if rc.Table == "*" {
			return errors.Errorf("wildcard * is not allowed for table name")
		}

		if regexp.QuoteMeta(rc.Table) != rc.Table { //通配符
			if _, ok := wildcards[global.RuleKey(rc.Schema, rc.Schema)]; ok {
				return errors.Errorf("duplicate wildcard table defined for %s.%s", rc.Schema, rc.Table)
			}

			tableName := rc.Table
			if rc.Table == "*" {
				tableName = "." + rc.Table
			}
			sql := fmt.Sprintf(`SELECT table_name FROM information_schema.tables WHERE
					table_name RLIKE "%s" AND table_schema = "%s";`, tableName, rc.Schema)
			res, err := s.canal.Execute(sql)
			if err != nil {
				return errors.Trace(err)
			}
			for i := 0; i < res.Resultset.RowNumber(); i++ {
				tableName, _ := res.GetString(i, 0)
				newRule, err := global.RuleDeepClone(rc)
				if err != nil {
					return errors.Trace(err)
				}
				newRule.Table = tableName
				ruleKey := global.RuleKey(rc.Schema, tableName)
				global.AddRuleIns(ruleKey, newRule)
			}
		} else {
			newRule, err := global.RuleDeepClone(rc)
			if err != nil {
				return errors.Trace(err)
			}
			ruleKey := global.RuleKey(rc.Schema, rc.Table)
			global.AddRuleIns(ruleKey, newRule)
		}
	}

	for _, rule := range global.RuleInsList() {
		tableMata, err := s.canal.GetTable(rule.Schema, rule.Table)
		if err != nil {
			return errors.Trace(err)
		}
		if len(tableMata.PKColumns) == 0 {
			if !global.Cfg().SkipNoPkTable {
				return errors.Errorf("%s.%s must have a PK for a column", rule.Schema, rule.Table)
			}
		}
		if len(tableMata.PKColumns) > 1 {
			rule.IsCompositeKey = true // 组合主键
		}
		rule.TableInfo = tableMata
		rule.TableColumnSize = len(tableMata.Columns)

		if err := rule.Initialize(); err != nil {
			return errors.Trace(err)
		}

		if rule.LuaEnable() {
			if err := rule.CompileLuaScript(global.Cfg().DataDir); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *TransferService) addDumpDatabaseOrTable() {
	var schema string
	schemas := make(map[string]int)
	tables := make([]string, 0, global.RuleInsTotal())
	for _, rule := range global.RuleInsList() {
		schema = rule.Table
		schemas[rule.Schema] = 1
		tables = append(tables, rule.Table)
	}
	if len(schemas) == 1 {
		s.canal.AddDumpTables(schema, tables...)
	} else {
		keys := make([]string, 0, len(schemas))
		for key := range schemas {
			keys = append(keys, key)
		}
		s.canal.AddDumpDatabases(keys...)
	}
}

func (s *TransferService) updateRule(schema, table string) error {
	rule, ok := global.RuleIns(global.RuleKey(schema, table))
	if ok {
		tableInfo, err := s.canal.GetTable(schema, table)
		if err != nil {
			return errors.Trace(err)
		}

		if len(tableInfo.PKColumns) == 0 {
			if !global.Cfg().SkipNoPkTable {
				return errors.Errorf("%s.%s must have a PK for a column", rule.Schema, rule.Table)
			}
		}

		if len(tableInfo.PKColumns) > 1 {
			rule.IsCompositeKey = true
		}

		rule.TableInfo = tableInfo
		rule.TableColumnSize = len(tableInfo.Columns)

		err = rule.AfterUpdateTableInfo()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *TransferService) startLoop() {
	go func() {
		ticker := time.NewTicker(_transferLoopInterval * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if !s.endpointEnable.Load() {
					err := s.endpoint.Ping()
					if err != nil {
						logs.Warn("destination not available, see the log file for details")
						logs.Error(err.Error())
					} else {
						s.endpointEnable.Store(true)
						if global.Cfg().IsRabbitmq() {
							s.endpoint.Connect()
						}
						s.StartUp()
						metrics.SetDestState(metrics.DestStateOK)
					}
				}
			case <-s.loopStopSignal:
				return
			}
		}
	}()
}
