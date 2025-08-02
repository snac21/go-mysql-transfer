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

// Package endpoint Kafka消息队列端点实现
// 提供与Apache Kafka集群的数据同步功能
// 支持SASL认证、异步生产者、分区策略等特性
package endpoint

import (
	"log"     // 标准日志
	"strings" // 字符串处理

	"github.com/IBM/sarama"                  // Kafka客户端库
	"github.com/go-mysql-org/go-mysql/canal" // Canal binlog解析
	"github.com/go-mysql-org/go-mysql/mysql" // MySQL协议和类型
	"github.com/juju/errors"                 // 错误处理增强

	"go-mysql-transfer/global"            // 全局配置和规则
	"go-mysql-transfer/metrics"           // 监控指标
	"go-mysql-transfer/model"             // 数据模型
	"go-mysql-transfer/service/luaengine" // Lua脚本引擎
	"go-mysql-transfer/util/logs"         // 日志工具
)

// KafkaEndpoint Kafka消息队列端点实现
// 负责将MySQL数据变更发送到Kafka主题
// 支持异步生产、SASL认证、自定义分区策略等功能
type KafkaEndpoint struct {
	client   sarama.Client        // Kafka客户端，用于集群连接和元数据管理
	producer sarama.AsyncProducer // 异步生产者，用于发送消息到Kafka
}

// newKafkaEndpoint 创建Kafka端点实例
// 返回初始化的KafkaEndpoint实例
func newKafkaEndpoint() *KafkaEndpoint {
	r := &KafkaEndpoint{}
	return r
}

// Connect 连接到Kafka集群
// 配置Kafka客户端和异步生产者，支持SASL认证
// 返回连接过程中可能出现的错误
func (s *KafkaEndpoint) Connect() error {
	// 1. 创建Kafka配置
	cfg := sarama.NewConfig()

	// 设置分区策略为随机分区，提高消息分布均匀性
	cfg.Producer.Partitioner = sarama.NewRandomPartitioner

	// 2. 配置SASL认证（如果需要）
	if global.Cfg().KafkaSASLUser != "" && global.Cfg().KafkaSASLPassword != "" {
		cfg.Net.SASL.Enable = true                             // 启用SASL认证
		cfg.Net.SASL.User = global.Cfg().KafkaSASLUser         // 设置用户名
		cfg.Net.SASL.Password = global.Cfg().KafkaSASLPassword // 设置密码
	}

	// 3. 解析Kafka集群地址
	var err error
	var client sarama.Client
	ls := strings.Split(global.Cfg().KafkaAddr, ",") // 解析逗号分隔的地址列表

	// 4. 创建Kafka客户端
	client, err = sarama.NewClient(ls, cfg)
	if err != nil {
		return errors.Errorf("unable to create kafka client: %q", err)
	}

	// 5. 创建异步生产者
	var producer sarama.AsyncProducer
	producer, err = sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		return errors.Errorf("unable to create kafka producer: %q", err)
	}

	// 6. 保存客户端和生产者实例
	s.producer = producer
	s.client = client

	return nil // 连接成功
}

// Ping 检查Kafka集群连接状态
// 通过刷新元数据来验证与Kafka集群的连接
// 返回连接检查的结果，nil表示连接正常
func (s *KafkaEndpoint) Ping() error {
	return s.client.RefreshMetadata() // 刷新集群元数据，验证连接
}

// Consume 消费增量数据，将MySQL变更发送到Kafka
// 处理binlog解析后的行变更请求，转换为Kafka消息并发送
// from: MySQL binlog位置信息
// rows: 行变更请求列表
// 返回数据消费过程中的错误
func (s *KafkaEndpoint) Consume(from mysql.Position, rows []*model.RowRequest) error {
	// 1. 准备消息列表
	var ms []*sarama.ProducerMessage

	// 2. 处理每个行变更请求
	for _, row := range rows {
		// 获取对应的同步规则
		rule, _ := global.RuleIns(row.RuleKey)

		// 检查表结构是否匹配
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue // 跳过不匹配的数据
		}

		// 更新监控指标
		metrics.UpdateActionNum(row.Action, row.RuleKey)

		// 3. 根据规则类型构建消息
		if rule.LuaEnable() {
			// 使用Lua脚本构建消息
			ls, err := s.buildMessages(row, rule)
			if err != nil {
				log.Println("Lua 脚本执行失败!!! ,详情请参见日志")
				return errors.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
			}
			ms = append(ms, ls...) // 添加Lua脚本生成的多个消息
		} else {
			// 使用内置规则构建消息
			m, err := s.buildMessage(row, rule)
			if err != nil {
				return errors.Errorf(errors.ErrorStack(err))
			}
			ms = append(ms, m) // 添加单个消息
		}
	}

	// 4. 发送所有消息到Kafka
	for _, m := range ms {
		// 将消息发送到异步生产者的输入通道
		s.producer.Input() <- m

		// 检查是否有发送错误
		select {
		case err := <-s.producer.Errors():
			return err // 返回发送错误
		default:
			// 没有错误，继续处理下一个消息
		}
	}

	logs.Infof("处理完成 %d 条数据", len(rows))
	return nil // 所有消息发送成功
}

// Stock 批量存储数据到Kafka（全量同步模式）
// 用于全量数据同步场景，批量处理行变更请求
// rows: 行变更请求列表
// 返回成功处理的数据行数，失败时返回0
func (s *KafkaEndpoint) Stock(rows []*model.RowRequest) int64 {
	expect := true // 处理期望标志，用于跟踪是否所有操作都成功

	// 遍历处理每个行变更请求
	for _, row := range rows {
		// 获取对应的同步规则
		rule, _ := global.RuleIns(row.RuleKey)

		// 检查表结构是否匹配，防止数据不一致
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey) // 记录警告日志
			continue                                         // 跳过不匹配的数据行
		}

		// 根据规则类型选择不同的消息构建方式
		if rule.LuaEnable() {
			// 使用Lua脚本构建多个消息
			ls, err := s.buildMessages(row, rule)
			if err != nil {
				logs.Errorf(errors.ErrorStack(err)) // 记录错误详情
				expect = false                      // 标记处理失败
				break                               // 退出处理循环
			}

			// 发送Lua脚本生成的所有消息
			for _, m := range ls {
				s.producer.Input() <- m // 发送消息到异步生产者

				// 检查发送是否出错
				select {
				case err := <-s.producer.Errors():
					logs.Error(err.Error()) // 记录发送错误
					expect = false          // 标记处理失败
					break                   // 退出内层循环
				default:
					// 没有错误，继续处理
				}
			}

			// 如果内层循环出错，退出外层循环
			if !expect {
				break
			}
		} else {
			// 使用内置规则构建单个消息
			m, err := s.buildMessage(row, rule)
			if err != nil {
				logs.Errorf(errors.ErrorStack(err)) // 记录错误详情
				expect = false                      // 标记处理失败
				break                               // 退出处理循环
			}

			// 发送消息到异步生产者
			s.producer.Input() <- m

			// 检查发送是否出错
			select {
			case err := <-s.producer.Errors():
				logs.Error(err.Error()) // 记录发送错误
				expect = false          // 标记处理失败
				break                   // 退出处理循环
			default:
				// 没有错误，继续处理
			}
		}
	}

	// 根据处理结果返回相应的值
	if !expect {
		return 0 // 处理失败，返回0
	}

	return int64(len(rows)) // 处理成功，返回处理的行数
}

// buildMessages 使用Lua脚本构建多个Kafka消息
// 通过Lua脚本引擎处理行数据，支持复杂的数据转换和多消息生成
// row: 行变更请求
// rule: 同步规则（包含Lua脚本配置）
// 返回构建的Kafka消息列表和可能的错误
func (s *KafkaEndpoint) buildMessages(row *model.RowRequest, rule *global.Rule) ([]*sarama.ProducerMessage, error) {
	var err error
	var ls []*model.MQRespond // MQ响应列表，Lua脚本可能生成多个消息

	// 将行数据转换为键值映射，便于Lua脚本处理
	kvm := rowMap(row, rule, true)

	// 根据操作类型调用Lua脚本
	if row.Action == canal.UpdateAction {
		// UPDATE操作：需要提供新旧数据对比
		previous := oldRowMap(row, rule, true)                       // 获取更新前的数据映射
		ls, err = luaengine.DoMQOps(kvm, previous, row.Action, rule) // 执行Lua脚本
	} else {
		// INSERT/DELETE操作：只需要当前数据
		ls, err = luaengine.DoMQOps(kvm, nil, row.Action, rule) // 执行Lua脚本
	}

	// 检查Lua脚本执行结果
	if err != nil {
		return nil, errors.Errorf("lua 脚本执行失败 : %s ", err)
	}

	// 将MQ响应转换为Kafka生产者消息
	var ms []*sarama.ProducerMessage
	for _, resp := range ls {
		// 创建Kafka生产者消息
		m := &sarama.ProducerMessage{
			Topic: resp.Topic,                         // 目标主题（由Lua脚本指定）
			Value: sarama.ByteEncoder(resp.ByteArray), // 消息内容（已序列化的字节数组）
		}

		// 记录消息发送日志
		logs.Infof("topic: %s, message: %s", resp.Topic, string(resp.ByteArray))
		ms = append(ms, m) // 添加到消息列表
	}

	return ms, nil // 返回构建的消息列表
}

// buildMessage 使用内置规则构建单个Kafka消息
// 将行变更请求转换为标准格式的Kafka消息
// row: 行变更请求
// rule: 同步规则
// 返回构建的Kafka消息和可能的错误
func (s *KafkaEndpoint) buildMessage(row *model.RowRequest, rule *global.Rule) (*sarama.ProducerMessage, error) {
	// 1. 构建数据映射
	kvm := rowMap(row, rule, false)

	// 2. 创建消息响应对象
	resp := new(model.MQRespond)
	resp.Action = row.Action       // 操作类型
	resp.Timestamp = row.Timestamp // 时间戳

	// 3. 根据编码规则设置数据内容
	if rule.ValueEncoder == global.ValEncoderJson {
		resp.Data = kvm // 直接使用JSON对象
	} else {
		resp.Data = encodeValue(rule, kvm) // 使用自定义编码
	}

	// 4. 如果需要保留原始数据且是UPDATE操作，添加旧数据
	if rule.ReserveRawData && canal.UpdateAction == row.Action {
		resp.Raw = oldRowMap(row, rule, false)
	}

	// 5. 序列化消息内容
	body, err := json.Marshal(resp)
	if err != nil {
		return nil, err // 序列化失败
	}

	// 6. 创建Kafka生产者消息
	m := &sarama.ProducerMessage{
		Topic: rule.KafkaTopic,          // 目标主题
		Value: sarama.ByteEncoder(body), // 消息内容
	}

	logs.Infof("topic: %s, message: %s", rule.KafkaTopic, string(body))
	return m, nil
}

// Close 关闭Kafka连接
// 依次关闭异步生产者和客户端，释放相关资源
func (s *KafkaEndpoint) Close() {
	// 关闭异步生产者
	if s.producer != nil {
		s.producer.Close()
	}

	// 关闭Kafka客户端
	if s.client != nil {
		s.client.Close()
	}
}
