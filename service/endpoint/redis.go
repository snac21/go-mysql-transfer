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

// Package endpoint Redis端点实现
// 提供与Redis数据库的数据同步功能
// 支持Redis的5种数据结构：String、Hash、List、Set、Sorted Set
// 支持单机、哨兵、集群三种部署模式
package endpoint

import (
	"bytes"   // 字节缓冲区，用于模板渲染
	"log"     // 标准日志
	"strings" // 字符串处理

	// 同步原语（未使用的retryLock字段）
	"github.com/go-mysql-org/go-mysql/canal" // Canal binlog解析
	"github.com/go-mysql-org/go-mysql/mysql" // MySQL协议和类型
	"github.com/go-redis/redis"              // Redis客户端库
	"github.com/pingcap/errors"              // 错误处理增强

	"go-mysql-transfer/global"            // 全局配置和规则
	"go-mysql-transfer/metrics"           // 监控指标
	"go-mysql-transfer/model"             // 数据模型
	"go-mysql-transfer/service/luaengine" // Lua脚本引擎
	"go-mysql-transfer/util/logs"         // 日志工具
	"go-mysql-transfer/util/stringutil"   // 字符串工具
)

// RedisEndpoint Redis端点实现结构体
// 负责将MySQL数据变更同步到Redis数据库
// 支持单机、哨兵、集群三种Redis部署模式
type RedisEndpoint struct {
	isCluster bool                 // 是否为集群模式标志
	client    *redis.Client        // 单机/哨兵模式Redis客户端
	cluster   *redis.ClusterClient // 集群模式Redis客户端
}

// newRedisEndpoint 创建Redis端点实例
// 根据配置自动选择单机、哨兵或集群模式
// 返回初始化完成的RedisEndpoint实例
func newRedisEndpoint() *RedisEndpoint {
	cfg := global.Cfg()   // 获取全局配置
	r := &RedisEndpoint{} // 创建Redis端点实例

	// 解析Redis地址列表，支持逗号分隔的多个地址
	list := strings.Split(cfg.RedisAddr, ",")

	if len(list) == 1 {
		// 单机模式：只有一个Redis地址
		r.client = redis.NewClient(&redis.Options{
			Addr:     cfg.RedisAddr,     // Redis服务器地址
			Password: cfg.RedisPass,     // Redis密码
			DB:       cfg.RedisDatabase, // Redis数据库编号（0-15）
		})
	} else {
		// 多地址模式：根据组类型选择哨兵或集群
		if cfg.RedisGroupType == global.RedisGroupTypeSentinel {
			// 哨兵模式：高可用主从架构
			r.client = redis.NewFailoverClient(&redis.FailoverOptions{
				MasterName:    cfg.RedisMasterName, // 主节点名称
				SentinelAddrs: list,                // 哨兵节点地址列表
				Password:      cfg.RedisPass,       // Redis密码
				DB:            cfg.RedisDatabase,   // 数据库编号
			})
		}
		if cfg.RedisGroupType == global.RedisGroupTypeCluster {
			// 集群模式：分布式Redis集群
			r.isCluster = true // 标记为集群模式
			r.cluster = redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:    list,          // 集群节点地址列表
				Password: cfg.RedisPass, // Redis密码
			})
		}
	}

	return r // 返回配置完成的Redis端点实例
}

// Connect 连接到Redis服务器
// 通过Ping操作验证连接是否正常
// 返回连接过程中可能出现的错误
func (s *RedisEndpoint) Connect() error {
	return s.Ping() // 直接调用Ping方法验证连接
}

// Ping 检查Redis连接状态
// 向Redis服务器发送PING命令验证连接
// 返回连接检查的结果，nil表示连接正常
func (s *RedisEndpoint) Ping() error {
	var err error
	if s.isCluster {
		// 集群模式：向集群发送PING命令
		_, err = s.cluster.Ping().Result()
	} else {
		// 单机/哨兵模式：向单个客户端发送PING命令
		_, err = s.client.Ping().Result()
	}
	return err // 返回PING操作的结果
}

// pipe 获取Redis管道对象
// 管道用于批量执行Redis命令，提高性能
// 返回对应模式的管道实例
func (s *RedisEndpoint) pipe() redis.Pipeliner {
	var pipe redis.Pipeliner
	if s.isCluster {
		// 集群模式：创建集群管道
		pipe = s.cluster.Pipeline()
	} else {
		// 单机/哨兵模式：创建普通管道
		pipe = s.client.Pipeline()
	}
	return pipe // 返回管道实例
}

// Consume 消费增量数据，将MySQL变更同步到Redis
// 处理binlog解析后的行变更请求，转换为Redis操作并批量执行
// from: MySQL binlog位置信息
// rows: 行变更请求列表
// 返回数据消费过程中的错误
func (s *RedisEndpoint) Consume(from mysql.Position, rows []*model.RowRequest) error {
	pipe := s.pipe() // 获取Redis管道，用于批量操作

	// 遍历处理每个行变更请求
	for _, row := range rows {
		// 获取对应的同步规则
		rule, _ := global.RuleIns(row.RuleKey)

		// 检查表结构是否匹配，防止数据不一致
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue // 跳过不匹配的数据行
		}

		// 更新监控指标，记录操作类型和规则
		metrics.UpdateActionNum(row.Action, row.RuleKey)

		// 根据规则类型选择不同的处理方式
		if rule.LuaEnable() {
			// 使用Lua脚本处理数据
			var err error
			var ls []*model.RedisRespond // Redis响应列表

			// 将行数据转换为键值映射
			kvm := rowMap(row, rule, true)

			// 根据操作类型调用Lua脚本
			if row.Action == canal.UpdateAction {
				// UPDATE操作：需要提供新旧数据对比
				previous := oldRowMap(row, rule, true)                          // 获取更新前的数据
				ls, err = luaengine.DoRedisOps(kvm, previous, row.Action, rule) // 执行Lua脚本
			} else {
				// INSERT/DELETE操作：只需要当前数据
				ls, err = luaengine.DoRedisOps(kvm, nil, row.Action, rule) // 执行Lua脚本
			}

			// 检查Lua脚本执行结果
			if err != nil {
				log.Println("Lua 脚本执行失败!!! ,详情请参见日志")
				return errors.Errorf("Lua 脚本执行失败 : %s ", errors.ErrorStack(err))
			}

			// 处理Lua脚本生成的所有Redis操作
			for _, resp := range ls {
				s.preparePipe(resp, pipe) // 将操作添加到管道
				// 记录操作详情日志
				logs.Infof("action: %s, structure: %s ,key: %s ,field: %s, value: %v",
					resp.Action, resp.Structure, resp.Key, resp.Field, resp.Val)
			}
			kvm = nil // 释放内存
		} else {
			// 使用内置规则处理数据
			resp := s.ruleRespond(row, rule) // 根据规则生成Redis响应
			s.preparePipe(resp, pipe)        // 将操作添加到管道
			// 记录操作详情日志
			logs.Infof("action: %s, structure: %s ,key: %s ,field: %s, value: %v",
				resp.Action, resp.Structure, resp.Key, resp.Field, resp.Val)
		}
	}

	// 批量执行管道中的所有Redis命令
	_, err := pipe.Exec()
	if err != nil {
		return err // 返回执行错误
	}

	logs.Infof("处理完成 %d 条数据", len(rows))
	return nil // 处理成功
}

// Stock 批量存储数据到Redis（全量同步模式）
// 用于全量数据同步场景，批量处理行变更请求
// rows: 行变更请求列表
// 返回成功处理的数据行数
func (s *RedisEndpoint) Stock(rows []*model.RowRequest) int64 {
	pipe := s.pipe() // 获取Redis管道，用于批量操作

	// 遍历处理每个行变更请求
	for _, row := range rows {
		// 获取对应的同步规则
		rule, _ := global.RuleIns(row.RuleKey)

		// 检查表结构是否匹配，防止数据不一致
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey) // 记录警告日志
			continue                                         // 跳过不匹配的数据行
		}

		// 根据规则类型选择不同的处理方式
		if rule.LuaEnable() {
			// 使用Lua脚本处理数据
			kvm := rowMap(row, rule, true)                              // 将行数据转换为键值映射
			ls, err := luaengine.DoRedisOps(kvm, nil, row.Action, rule) // 执行Lua脚本
			if err != nil {
				logs.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err)) // 记录错误
				break                                                   // 出错时退出处理循环
			}
			// 将Lua脚本生成的所有操作添加到管道
			for _, resp := range ls {
				s.preparePipe(resp, pipe)
			}
		} else {
			// 使用内置规则处理数据
			resp := s.ruleRespond(row, rule)     // 根据规则生成Redis响应
			resp.Action = row.Action             // 设置操作类型
			resp.Structure = rule.RedisStructure // 设置Redis数据结构类型
			s.preparePipe(resp, pipe)            // 将操作添加到管道
		}
	}

	var counter int64 // 成功操作计数器

	// 批量执行管道中的所有Redis命令
	res, err := pipe.Exec()
	if err != nil {
		logs.Error(err.Error()) // 记录执行错误
	}

	// 统计成功执行的命令数量
	for _, re := range res {
		if re.Err() == nil {
			counter++ // 命令执行成功，计数器加1
		}
	}

	return counter // 返回成功处理的数据行数
}

// ruleRespond 根据内置规则生成Redis响应
// 将行变更请求转换为Redis操作响应，处理不同数据结构的特殊需求
// row: 行变更请求
// rule: 同步规则
// 返回构建完成的Redis响应对象
func (s *RedisEndpoint) ruleRespond(row *model.RowRequest, rule *global.Rule) *model.RedisRespond {
	resp := new(model.RedisRespond)      // 创建Redis响应对象
	resp.Action = row.Action             // 设置操作类型（INSERT/UPDATE/DELETE）
	resp.Structure = rule.RedisStructure // 设置Redis数据结构类型

	// 将行数据转换为键值映射
	kvm := rowMap(row, rule, false)

	// 编码Redis键名
	resp.Key = s.encodeKey(row, rule)

	// Hash结构需要设置字段名
	if resp.Structure == global.RedisStructureHash {
		resp.Field = s.encodeHashField(row, rule)
	}

	// Sorted Set结构需要设置分数
	if resp.Structure == global.RedisStructureSortedSet {
		resp.Score = s.encodeSortedSetScoreField(row, rule)
	}

	// 根据操作类型设置值
	switch resp.Action {
	case canal.InsertAction:
		// INSERT操作：直接编码当前值
		resp.Val = encodeValue(rule, kvm)

	case canal.UpdateAction:
		// UPDATE操作：对于集合类型需要先删除旧值再添加新值
		if rule.RedisStructure == global.RedisStructureList ||
			rule.RedisStructure == global.RedisStructureSet ||
			rule.RedisStructure == global.RedisStructureSortedSet {
			// 获取更新前的数据并编码为旧值
			oldKvm := oldRowMap(row, rule, false)
			resp.OldVal = encodeValue(rule, oldKvm)
		}
		// 编码新值
		resp.Val = encodeValue(rule, kvm)

	default: // DELETE操作
		// DELETE操作：对于集合类型需要指定要删除的值
		if rule.RedisStructure == global.RedisStructureList ||
			rule.RedisStructure == global.RedisStructureSet ||
			rule.RedisStructure == global.RedisStructureSortedSet {
			resp.Val = encodeValue(rule, kvm)
		}
	}

	return resp // 返回构建完成的Redis响应
}

// preparePipe 根据Redis响应准备管道命令
// 将Redis响应转换为具体的Redis命令并添加到管道中
// 支持Redis的5种数据结构的所有操作
// resp: Redis响应对象
// pipe: Redis管道对象
func (s *RedisEndpoint) preparePipe(resp *model.RedisRespond, pipe redis.Cmdable) {
	// 根据Redis数据结构类型选择相应的操作
	switch resp.Structure {
	case global.RedisStructureString:
		// String结构：键值对存储
		if resp.Action == canal.DeleteAction {
			pipe.Del(resp.Key) // DELETE操作：删除整个键
		} else {
			pipe.Set(resp.Key, resp.Val, 0) // INSERT/UPDATE操作：设置键值（无过期时间）
		}

	case global.RedisStructureHash:
		// Hash结构：哈希表存储，类似于对象
		if resp.Action == canal.DeleteAction {
			pipe.HDel(resp.Key, resp.Field) // DELETE操作：删除哈希表中的指定字段
		} else {
			pipe.HSet(resp.Key, resp.Field, resp.Val) // INSERT/UPDATE操作：设置哈希字段值
		}

	case global.RedisStructureList:
		// List结构：有序列表存储
		if resp.Action == canal.DeleteAction {
			pipe.LRem(resp.Key, 0, resp.Val) // DELETE操作：从列表中删除所有匹配的值
		} else if resp.Action == canal.UpdateAction {
			pipe.LRem(resp.Key, 0, resp.OldVal) // UPDATE操作：先删除旧值
			pipe.RPush(resp.Key, resp.Val)      // 然后在列表右端添加新值
		} else {
			pipe.RPush(resp.Key, resp.Val) // INSERT操作：在列表右端添加值
		}

	case global.RedisStructureSet:
		// Set结构：无序集合存储，自动去重
		if resp.Action == canal.DeleteAction {
			pipe.SRem(resp.Key, resp.Val) // DELETE操作：从集合中删除指定值
		} else if resp.Action == canal.UpdateAction {
			pipe.SRem(resp.Key, resp.OldVal) // UPDATE操作：先删除旧值
			pipe.SAdd(resp.Key, resp.Val)    // 然后添加新值到集合
		} else {
			pipe.SAdd(resp.Key, resp.Val) // INSERT操作：添加值到集合
		}

	case global.RedisStructureSortedSet:
		// Sorted Set结构：有序集合存储，按分数排序
		if resp.Action == canal.DeleteAction {
			pipe.ZRem(resp.Key, resp.Val) // DELETE操作：从有序集合中删除指定成员
		} else if resp.Action == canal.UpdateAction {
			pipe.ZRem(resp.Key, resp.OldVal)                    // UPDATE操作：先删除旧成员
			val := redis.Z{Score: resp.Score, Member: resp.Val} // 创建带分数的成员
			pipe.ZAdd(resp.Key, val)                            // 添加新成员到有序集合
		} else {
			val := redis.Z{Score: resp.Score, Member: resp.Val} // 创建带分数的成员
			pipe.ZAdd(resp.Key, val)                            // INSERT操作：添加成员到有序集合
		}
	}
}

// encodeKey 编码Redis键名
// 根据规则配置生成Redis键，支持固定值、模板格式化、列值拼接等方式
// req: 行变更请求
// rule: 同步规则
// 返回编码后的Redis键名
func (s *RedisEndpoint) encodeKey(req *model.RowRequest, rule *global.Rule) string {
	// 方式1：使用固定的键值
	if rule.RedisKeyValue != "" {
		return rule.RedisKeyValue // 直接返回配置的固定键值
	}

	// 方式2：使用模板格式化键名
	if rule.RedisKeyFormatter != "" {
		kv := rowMap(req, rule, true) // 将行数据转换为键值映射
		var tmplBytes bytes.Buffer    // 创建字节缓冲区
		// 执行模板渲染，将数据填充到模板中
		err := rule.RedisKeyTmpl.Execute(&tmplBytes, kv)
		if err != nil {
			return "" // 模板执行失败，返回空字符串
		}
		return tmplBytes.String() // 返回渲染后的键名
	}

	// 方式3：使用列值拼接键名
	var key string
	if rule.RedisKeyColumnIndex < 0 {
		// 多列拼接：将多个列的值连接成键名
		for _, v := range rule.RedisKeyColumnIndexs {
			key += stringutil.ToString(req.Row[v]) // 将列值转换为字符串并拼接
		}
	} else {
		// 单列键名：使用指定列的值作为键名
		key = stringutil.ToString(req.Row[rule.RedisKeyColumnIndex])
	}

	// 添加键名前缀（如果配置了）
	if rule.RedisKeyPrefix != "" {
		key = rule.RedisKeyPrefix + key // 在键名前添加前缀
	}

	return key // 返回最终的键名
}

// encodeHashField 编码Redis Hash结构的字段名
// 根据规则配置生成Hash字段名，支持单列或多列拼接
// req: 行变更请求
// rule: 同步规则
// 返回编码后的Hash字段名
func (s *RedisEndpoint) encodeHashField(req *model.RowRequest, rule *global.Rule) string {
	var field string

	if rule.RedisHashFieldColumnIndex < 0 {
		// 多列拼接：将多个列的值连接成字段名
		for _, v := range rule.RedisHashFieldColumnIndexs {
			field += stringutil.ToString(req.Row[v]) // 将列值转换为字符串并拼接
		}
	} else {
		// 单列字段名：使用指定列的值作为字段名
		field = stringutil.ToString(req.Row[rule.RedisHashFieldColumnIndex])
	}

	// 添加字段名前缀（如果配置了）
	if rule.RedisHashFieldPrefix != "" {
		field = rule.RedisHashFieldPrefix + field // 在字段名前添加前缀
	}

	return field // 返回最终的字段名
}

// encodeSortedSetScoreField 编码Redis Sorted Set结构的分数
// 从指定列获取数值作为有序集合的分数
// req: 行变更请求
// rule: 同步规则
// 返回转换后的分数值（float64类型）
func (s *RedisEndpoint) encodeSortedSetScoreField(req *model.RowRequest, rule *global.Rule) float64 {
	// 获取指定列的值作为分数
	obj := req.Row[rule.RedisHashFieldColumnIndex]
	if obj == nil {
		return 0 // 如果值为空，返回0分
	}

	// 将值转换为字符串，然后安全转换为float64
	str := stringutil.ToString(obj)
	return stringutil.ToFloat64Safe(str) // 安全转换，失败时返回0
}

// Close 关闭Redis连接
// 释放Redis客户端资源，清理连接
func (s *RedisEndpoint) Close() {
	if s.client != nil {
		s.client.Close() // 关闭单机/哨兵模式客户端
	}
	// 注意：这里缺少对集群客户端的关闭处理
	// 应该添加：if s.cluster != nil { s.cluster.Close() }
}
