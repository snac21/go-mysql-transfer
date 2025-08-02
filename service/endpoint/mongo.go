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

// Package endpoint MongoDB数据库端点实现
// 提供与MongoDB数据库的数据同步功能
// 支持副本集、认证、批量操作、重复键处理等特性
package endpoint

import (
	"context" // 上下文管理
	"strings" // 字符串处理
	"sync"    // 同步原语

	"github.com/go-mysql-org/go-mysql/canal"     // Canal binlog解析
	"github.com/go-mysql-org/go-mysql/mysql"     // MySQL协议和类型
	"github.com/juju/errors"                     // 错误处理增强
	"go.mongodb.org/mongo-driver/bson"           // BSON编码解码
	"go.mongodb.org/mongo-driver/mongo"          // MongoDB官方驱动
	"go.mongodb.org/mongo-driver/mongo/options"  // MongoDB连接选项
	"go.mongodb.org/mongo-driver/mongo/readpref" // MongoDB读偏好

	"go-mysql-transfer/global"            // 全局配置和规则
	"go-mysql-transfer/metrics"           // 监控指标
	"go-mysql-transfer/model"             // 数据模型
	"go-mysql-transfer/service/luaengine" // Lua脚本引擎
	"go-mysql-transfer/util/logs"         // 日志工具
	"go-mysql-transfer/util/stringutil"   // 字符串工具
)

// cKey 集合键结构体
// 用于唯一标识MongoDB中的数据库和集合组合
type cKey struct {
	database   string // 数据库名称
	collection string // 集合名称
}

// MongoEndpoint MongoDB数据库端点实现
// 负责将MySQL数据同步到MongoDB数据库
// 支持批量操作、连接池管理、重复键处理等功能
type MongoEndpoint struct {
	options     *options.ClientOptions     // MongoDB客户端连接选项
	client      *mongo.Client              // MongoDB客户端实例
	collections map[cKey]*mongo.Collection // 集合缓存，避免重复创建集合对象
	collLock    sync.RWMutex               // 集合缓存读写锁
}

// newMongoEndpoint 创建MongoDB端点实例
// 解析配置中的MongoDB连接信息，初始化客户端选项
// 返回配置完成的MongoEndpoint实例
func newMongoEndpoint() *MongoEndpoint {
	// 1. 解析MongoDB服务器地址列表（逗号分隔）
	addrList := strings.Split(global.Cfg().MongodbAddr, ",")

	// 2. 创建客户端连接选项
	opts := &options.ClientOptions{
		Hosts: addrList, // 设置MongoDB服务器地址列表
	}

	// 3. 如果配置了认证信息，添加认证凭据
	if global.Cfg().MongodbUsername != "" && global.Cfg().MongodbPassword != "" {
		opts.Auth = &options.Credential{
			Username: global.Cfg().MongodbUsername, // 用户名
			Password: global.Cfg().MongodbPassword, // 密码
		}
	}

	// 4. 创建端点实例并初始化
	r := &MongoEndpoint{}
	r.options = opts                                 // 保存连接选项
	r.collections = make(map[cKey]*mongo.Collection) // 初始化集合缓存
	return r
}

// Connect 连接到MongoDB数据库
// 建立MongoDB连接并预加载所有规则对应的集合对象
// 返回连接过程中可能出现的错误
func (s *MongoEndpoint) Connect() error {
	// 1. 建立MongoDB连接
	client, err := mongo.Connect(context.Background(), s.options)
	if err != nil {
		return err // 连接失败
	}

	s.client = client // 保存客户端实例

	// 2. 预加载所有规则对应的集合对象
	s.collLock.Lock()
	for _, rule := range global.RuleInsList() {
		// 获取数据库和集合对象
		cc := s.client.Database(rule.MongodbDatabase).Collection(rule.MongodbCollection)

		// 缓存集合对象，避免重复创建
		s.collections[s.collectionKey(rule.MongodbDatabase, rule.MongodbCollection)] = cc
	}
	s.collLock.Unlock()

	return nil // 连接成功
}

// Ping 检查MongoDB连接状态
// 向主节点发送ping命令验证连接
// 返回连接检查的结果，nil表示连接正常
func (s *MongoEndpoint) Ping() error {
	return s.client.Ping(context.Background(), readpref.Primary())
}

// isDuplicateKeyError 检查是否为重复键错误
// MongoDB在插入重复主键时会返回E11000错误
// stack: 错误堆栈信息
// 返回是否为重复键错误
func (s *MongoEndpoint) isDuplicateKeyError(stack string) bool {
	return strings.Contains(stack, "E11000 duplicate key error")
}

// collectionKey 创建集合键
// 将数据库名和集合名组合成唯一的键，用于集合缓存
// database: 数据库名称
// collection: 集合名称
// 返回集合键结构体
func (s *MongoEndpoint) collectionKey(database, collection string) cKey {
	return cKey{
		database:   database,
		collection: collection,
	}
}

// collection 获取MongoDB集合对象
// 首先从缓存中查找，如果不存在则创建并缓存
// 使用读写锁确保并发安全
// key: 集合键
// 返回MongoDB集合对象
func (s *MongoEndpoint) collection(key cKey) *mongo.Collection {
	// 1. 尝试从缓存中获取集合对象（读锁）
	s.collLock.RLock()
	c, ok := s.collections[key]
	s.collLock.RUnlock()
	if ok {
		return c // 缓存命中，直接返回
	}

	// 2. 缓存未命中，创建新的集合对象（写锁）
	s.collLock.Lock()
	c = s.client.Database(key.database).Collection(key.collection)
	s.collections[key] = c // 缓存新创建的集合对象
	s.collLock.Unlock()

	return c
}

// Consume 消费增量数据，将MySQL变更同步到MongoDB
// 处理binlog解析后的行变更请求，转换为MongoDB批量操作并执行
// from: MySQL binlog位置信息
// rows: 行变更请求列表
// 返回数据消费过程中的错误
func (s *MongoEndpoint) Consume(from mysql.Position, rows []*model.RowRequest) error {
	// 按集合分组的批量写入模型映射
	models := make(map[cKey][]mongo.WriteModel, 0)

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
			kvm := rowMap(row, rule, true)                         // 将行数据转换为键值映射
			ls, err := luaengine.DoMongoOps(kvm, row.Action, rule) // 执行Lua脚本
			if err != nil {
				return errors.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
			}

			// 处理Lua脚本生成的所有MongoDB操作
			for _, resp := range ls {
				var model mongo.WriteModel

				// 根据操作类型创建相应的MongoDB写入模型
				switch resp.Action {
				case canal.InsertAction:
					// INSERT操作：插入新文档
					model = mongo.NewInsertOneModel().SetDocument(resp.Table)

				case canal.UpdateAction:
					// UPDATE操作：更新现有文档
					model = mongo.NewUpdateOneModel().
						SetFilter(bson.M{"_id": resp.Id}).
						SetUpdate(bson.M{"$set": resp.Table})

				case global.UpsertAction:
					// UPSERT操作：不存在则插入，存在则更新
					model = mongo.NewUpdateOneModel().
						SetFilter(bson.M{"_id": resp.Id}).
						SetUpsert(true).
						SetUpdate(bson.M{"$set": resp.Table})

				case canal.DeleteAction:
					// DELETE操作：删除文档
					model = mongo.NewDeleteOneModel().SetFilter(bson.M{"_id": resp.Id})
				}

				// 按集合分组批量操作
				key := s.collectionKey(rule.MongodbDatabase, resp.Collection)
				array, ok := models[key]
				if !ok {
					array = make([]mongo.WriteModel, 0) // 初始化操作数组
				}

				// 记录操作详情日志
				logs.Infof("action:%s, collection:%s, id:%v, data:%v",
					resp.Action, resp.Collection, resp.Id, resp.Table)

				array = append(array, model) // 添加操作到数组
				models[key] = array          // 更新集合的操作数组
			}
		} else {
			// 使用内置规则处理数据
			kvm := rowMap(row, rule, false) // 将行数据转换为键值映射
			id := primaryKey(row, rule)     // 获取主键作为文档ID
			kvm["_id"] = id                 // 设置MongoDB文档ID

			var model mongo.WriteModel

			// 根据操作类型创建相应的MongoDB写入模型
			switch row.Action {
			case canal.InsertAction:
				// INSERT操作：插入新文档
				model = mongo.NewInsertOneModel().SetDocument(kvm)

			case canal.UpdateAction:
				// UPDATE操作：更新现有文档
				model = mongo.NewUpdateOneModel().
					SetFilter(bson.M{"_id": id}).
					SetUpdate(bson.M{"$set": kvm})

			case canal.DeleteAction:
				// DELETE操作：删除文档
				model = mongo.NewDeleteOneModel().SetFilter(bson.M{"_id": id})
			}

			// 按集合分组批量操作
			ccKey := s.collectionKey(rule.MongodbDatabase, rule.MongodbCollection)
			array, ok := models[ccKey]
			if !ok {
				array = make([]mongo.WriteModel, 0) // 初始化操作数组
			}

			// 记录操作详情日志
			logs.Infof("action:%s, collection:%s, id:%v, data:%v",
				row.Action, rule.MongodbCollection, id, kvm)

			array = append(array, model) // 添加操作到数组
			models[ccKey] = array        // 更新集合的操作数组
		}
	}

	var slowly bool // 慢速处理标志，用于重复键错误处理

	// 按集合执行批量写入操作
	for key, model := range models {
		collection := s.collection(key) // 获取MongoDB集合对象

		// 执行批量写入操作
		_, err := collection.BulkWrite(context.Background(), model)
		if err != nil {
			// 检查是否为重复键错误
			if s.isDuplicateKeyError(err.Error()) {
				slowly = true // 标记需要慢速处理
			} else {
				return err // 其他错误直接返回
			}
			logs.Error(errors.ErrorStack(err))
			break // 出错时退出循环
		}
	}
	// 如果遇到重复键错误，使用慢速处理模式
	if slowly {
		_, err := s.doConsumeSlowly(rows) // 逐条处理，跳过重复键错误
		if err != nil {
			return err
		}
	}

	logs.Infof("处理完成 %d 条数据", len(rows))
	return nil // 处理成功
}

// Stock 批量存储数据到MongoDB（全量同步模式）
// 用于全量数据同步场景，批量处理行变更请求，主要用于INSERT操作
// rows: 行变更请求列表
// 返回成功处理的数据行数
func (s *MongoEndpoint) Stock(rows []*model.RowRequest) int64 {
	expect := true // 处理期望标志，用于跟踪是否所有操作都成功

	// 按集合分组的批量写入模型映射
	models := make(map[cKey][]mongo.WriteModel, 0)

	// 遍历处理每个行变更请求
	for _, row := range rows {
		// 获取对应的同步规则
		rule, _ := global.RuleIns(row.RuleKey)

		// 检查表结构是否匹配，防止数据不一致
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue // 跳过不匹配的数据行
		}

		// 根据规则类型选择不同的处理方式
		if rule.LuaEnable() {
			// 使用Lua脚本处理数据
			kvm := rowMap(row, rule, true)                         // 将行数据转换为键值映射
			ls, err := luaengine.DoMongoOps(kvm, row.Action, rule) // 执行Lua脚本
			if err != nil {
				logs.Error("Lua 脚本执行失败!!! ,详情请参见日志")
				logs.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
				expect = false // 标记处理失败
				break          // 退出处理循环
			}

			// 处理Lua脚本生成的所有MongoDB操作（全量同步主要是INSERT）
			for _, resp := range ls {
				ccKey := s.collectionKey(rule.MongodbDatabase, resp.Collection)
				model := mongo.NewInsertOneModel().SetDocument(resp.Table) // 创建插入模型

				array, ok := models[ccKey]
				if !ok {
					array = make([]mongo.WriteModel, 0) // 初始化操作数组
				}
				array = append(array, model) // 添加操作到数组
				models[ccKey] = array        // 更新集合的操作数组
			}
		} else {
			// 使用内置规则处理数据
			kvm := rowMap(row, rule, false) // 将行数据转换为键值映射
			id := primaryKey(row, rule)     // 获取主键作为文档ID
			kvm["_id"] = id                 // 设置MongoDB文档ID

			ccKey := s.collectionKey(rule.MongodbDatabase, rule.MongodbCollection)
			model := mongo.NewInsertOneModel().SetDocument(kvm) // 创建插入模型

			array, ok := models[ccKey]
			if !ok {
				array = make([]mongo.WriteModel, 0) // 初始化操作数组
			}
			array = append(array, model) // 添加操作到数组
			models[ccKey] = array        // 更新集合的操作数组
		}
	}

	// 如果处理过程中出现错误，返回0
	if !expect {
		return 0
	}

	var slowly bool // 慢速处理标志
	var sum int64   // 成功插入的文档计数

	// 按集合执行批量写入操作
	for key, vs := range models {
		collection := s.collection(key) // 获取MongoDB集合对象

		// 执行批量写入操作
		rr, err := collection.BulkWrite(context.Background(), vs)
		if err != nil {
			// 检查是否为重复键错误
			if s.isDuplicateKeyError(err.Error()) {
				slowly = true // 标记需要慢速处理
			}
			logs.Error(errors.ErrorStack(err))
			break // 出错时退出循环
		}
		sum += rr.InsertedCount // 累计成功插入的文档数
	}

	// 如果遇到重复键错误，使用慢速处理模式
	if slowly {
		logs.Info("do consume slowly ... ... ")
		slowlySum, err := s.doConsumeSlowly(rows) // 逐条处理，跳过重复键错误
		if err != nil {
			logs.Warnf(err.Error())
		}
		return slowlySum // 返回慢速处理的结果
	}

	return sum // 返回成功处理的数据行数
}

func (s *MongoEndpoint) doConsumeSlowly(rows []*model.RowRequest) (int64, error) {
	var sum int64
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		if rule.LuaEnable() {
			kvm := rowMap(row, rule, true)
			ls, err := luaengine.DoMongoOps(kvm, row.Action, rule)
			if err != nil {
				logs.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
				return sum, err
			}
			for _, resp := range ls {
				collection := s.collection(s.collectionKey(rule.MongodbDatabase, resp.Collection))
				switch resp.Action {
				case canal.InsertAction:
					_, err := collection.InsertOne(context.Background(), resp.Table)
					if err != nil {
						if s.isDuplicateKeyError(err.Error()) {
							logs.Warnf("duplicate key [ %v ]", stringutil.ToJsonString(resp.Table))
						} else {
							return sum, err
						}
					}
				case canal.UpdateAction:
					_, err := collection.UpdateOne(context.Background(), bson.M{"_id": resp.Id}, bson.M{"$set": resp.Table})
					if err != nil {
						return sum, err
					}
				case canal.DeleteAction:
					_, err := collection.DeleteOne(context.Background(), bson.M{"_id": resp.Id})
					if err != nil {
						return sum, err
					}
				}
				logs.Infof("action:%s, collection:%s, id:%v, data:%v",
					row.Action, collection.Name(), resp.Id, resp.Table)
			}
		} else {
			kvm := rowMap(row, rule, false)
			id := primaryKey(row, rule)
			kvm["_id"] = id

			collection := s.collection(s.collectionKey(rule.MongodbDatabase, rule.MongodbCollection))

			switch row.Action {
			case canal.InsertAction:
				_, err := collection.InsertOne(context.Background(), kvm)
				if err != nil {
					if s.isDuplicateKeyError(err.Error()) {
						logs.Warnf("duplicate key [ %v ]", stringutil.ToJsonString(kvm))
					} else {
						return sum, err
					}
				}
			case canal.UpdateAction:
				_, err := collection.UpdateOne(context.Background(), bson.M{"_id": id}, bson.M{"$set": kvm})
				if err != nil {
					return sum, err
				}
			case canal.DeleteAction:
				_, err := collection.DeleteOne(context.Background(), bson.M{"_id": id})
				if err != nil {
					return sum, err
				}
			}

			logs.Infof("action:%s, collection:%s, id:%v, data:%v", row.Action, collection.Name(), id, kvm)
		}
		sum++
	}
	return sum, nil
}

// Close 关闭MongoDB连接
// 断开与MongoDB的连接，释放相关资源
func (s *MongoEndpoint) Close() {
	if s.client != nil {
		s.client.Disconnect(context.Background()) // 断开MongoDB连接
	}
}
