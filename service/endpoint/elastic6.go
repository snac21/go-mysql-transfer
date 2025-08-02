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

// Package endpoint Elasticsearch 6.x版本端点实现
// 提供与Elasticsearch 6.x集群的数据同步功能
// 支持索引自动创建、映射管理、批量操作等特性
package endpoint

import (
	"context" // 上下文管理
	"strings" // 字符串处理
	"sync"    // 同步原语

	"github.com/go-mysql-org/go-mysql/canal" // Canal binlog解析
	"github.com/go-mysql-org/go-mysql/mysql" // MySQL协议和类型
	"github.com/juju/errors"                 // 错误处理增强
	"github.com/olivere/elastic"             // Elasticsearch 6.x客户端

	"go-mysql-transfer/global"            // 全局配置和规则
	"go-mysql-transfer/metrics"           // 监控指标
	"go-mysql-transfer/model"             // 数据模型
	"go-mysql-transfer/service/luaengine" // Lua脚本引擎
	"go-mysql-transfer/util/logagent"     // 日志代理
	"go-mysql-transfer/util/logs"         // 日志工具
	"go-mysql-transfer/util/stringutil"   // 字符串工具
)

// Elastic6Endpoint Elasticsearch 6.x端点实现
// 负责将MySQL数据同步到Elasticsearch 6.x集群
// 支持多节点集群、自动索引管理、批量操作等功能
type Elastic6Endpoint struct {
	first  string          // 第一个ES节点地址，用于健康检查
	hosts  []string        // ES集群节点地址列表
	client *elastic.Client // ES客户端实例

	retryLock sync.Mutex // 重试操作锁，防止并发重试
}

// newElastic6Endpoint 创建Elasticsearch 6.x端点实例
// 解析配置中的ES集群地址，初始化端点结构
// 返回配置完成的Elastic6Endpoint实例
func newElastic6Endpoint() *Elastic6Endpoint {
	r := &Elastic6Endpoint{}

	// 解析ES集群地址配置（逗号分隔的多个地址）
	r.hosts = strings.Split(global.Cfg().ElsAddr, ",")

	// 设置第一个节点作为健康检查的默认节点
	r.first = r.hosts[0]

	return r
}

// Connect 连接到Elasticsearch集群
// 配置ES客户端选项，建立连接，并初始化索引映射
// 返回连接过程中可能出现的错误
func (s *Elastic6Endpoint) Connect() error {
	// 1. 配置ES客户端选项
	var options []elastic.ClientOptionFunc

	// 设置错误日志代理
	options = append(options, elastic.SetErrorLog(logagent.NewElsLoggerAgent()))

	// 设置ES集群节点URL列表
	options = append(options, elastic.SetURL(s.hosts...))

	// 如果配置了认证信息，添加基本认证
	if global.Cfg().ElsUser != "" && global.Cfg().ElsPassword != "" {
		options = append(options, elastic.SetBasicAuth(global.Cfg().ElsUser, global.Cfg().ElsPassword))
	}

	// 2. 创建ES客户端
	client, err := elastic.NewClient(options...)
	if err != nil {
		return err // 客户端创建失败
	}

	// 3. 保存客户端实例并初始化索引映射
	s.client = client
	return s.indexMapping() // 初始化所有规则对应的索引映射
}

// indexMapping 初始化所有规则对应的索引映射
// 遍历所有同步规则，检查对应的ES索引是否存在
// 如果索引不存在则创建，如果存在则更新映射
// 返回映射初始化过程中的错误
func (s *Elastic6Endpoint) indexMapping() error {
	// 遍历所有同步规则
	for _, rule := range global.RuleInsList() {
		// 1. 检查索引是否存在
		exists, err := s.client.IndexExists(rule.ElsIndex).Do(context.Background())
		if err != nil {
			return err // 检查索引存在性失败
		}

		// 2. 根据索引存在性决定操作类型
		if exists {
			// 索引已存在，更新映射（添加新字段）
			err = s.updateIndexMapping(rule)
		} else {
			// 索引不存在，创建新索引和映射
			err = s.insertIndexMapping(rule)
		}

		if err != nil {
			return err // 映射操作失败
		}
	}

	return nil // 所有索引映射初始化完成
}

func (s *Elastic6Endpoint) insertIndexMapping(rule *global.Rule) error {
	var properties map[string]interface{}
	if rule.LuaEnable() {
		properties = buildPropertiesByMappings(rule)
	} else {
		properties = buildPropertiesByRule(rule)
	}

	mapping := map[string]interface{}{
		"mappings": map[string]interface{}{
			rule.ElsType: map[string]interface{}{
				"properties": properties,
			},
		},
	}
	body := stringutil.ToJsonString(mapping)

	ret, err := s.client.CreateIndex(rule.ElsIndex).Body(body).Do(context.Background())
	if err != nil {
		return err
	}
	if !ret.Acknowledged {
		return errors.Errorf("create index %s err", rule.ElsIndex)
	}
	logs.Infof("create index succeed, index: %s", body)

	return nil
}

func (s *Elastic6Endpoint) updateIndexMapping(rule *global.Rule) error {
	ret, err := s.client.GetMapping().Index(rule.ElsIndex).Do(context.Background())
	if err != nil {
		return err
	}

	if ret[rule.ElsIndex] == nil {
		return nil
	}
	retIndex := ret[rule.ElsIndex].(map[string]interface{})

	if retIndex["mappings"] == nil {
		return nil
	}
	retMaps := retIndex["mappings"].(map[string]interface{})

	if retMaps["_doc"] == nil {
		return nil
	}
	retDoc := retMaps["_doc"].(map[string]interface{})

	if retDoc["properties"] == nil {
		return nil
	}
	retPros := retDoc["properties"].(map[string]interface{})

	var currents map[string]interface{}
	if rule.LuaEnable() {
		currents = buildPropertiesByMappings(rule)
	} else {
		currents = buildPropertiesByRule(rule)
	}

	if len(retPros) < len(currents) {
		properties := make(map[string]interface{})
		mapping := map[string]interface{}{
			"properties": properties,
		}
		for field, current := range currents {
			if _, exist := retPros[field]; !exist {
				properties[field] = current
			}
		}
		doc := stringutil.ToJsonString(mapping)
		ret, err := s.client.PutMapping().Index(rule.ElsIndex).Type(rule.ElsType).BodyString(doc).Do(context.Background())
		if err != nil {
			return err
		}
		if !ret.Acknowledged {
			return errors.Errorf("update index %s err", rule.ElsIndex)
		}
		logs.Infof("update index succeed, index: %s", doc)
	}

	return nil
}

// Ping 检查Elasticsearch集群连接状态
// 首先尝试ping第一个节点，如果失败则遍历所有节点
// 只要有一个节点可达就认为集群可用
// 返回连接检查的结果，nil表示连接正常
func (s *Elastic6Endpoint) Ping() error {
	// 1. 优先检查第一个节点（通常是主节点）
	if _, _, err := s.client.Ping(s.first).Do(context.Background()); err == nil {
		return nil // 第一个节点可达，连接正常
	}

	// 2. 如果第一个节点不可达，遍历检查所有节点
	for _, host := range s.hosts {
		if _, _, err := s.client.Ping(host).Do(context.Background()); err == nil {
			return nil // 找到可达节点，连接正常
		}
	}

	// 3. 所有节点都不可达，返回连接错误
	return errors.New("elasticsearch cluster is not reachable")
}

// Consume 消费增量数据，将MySQL变更同步到Elasticsearch
// 处理binlog解析后的行变更请求，支持INSERT、UPDATE、DELETE操作
// from: MySQL binlog位置信息
// rows: 行变更请求列表
// 返回数据消费过程中的错误
func (s *Elastic6Endpoint) Consume(from mysql.Position, rows []*model.RowRequest) error {
	// 1. 创建批量操作对象
	bulk := s.client.Bulk()

	// 2. 处理每个行变更请求
	for _, row := range rows {
		// 获取对应的同步规则
		rule, _ := global.RuleIns(row.RuleKey)

		// 检查表结构是否匹配（防止表结构变更导致的数据错误）
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue // 跳过不匹配的数据
		}

		// 更新监控指标
		metrics.UpdateActionNum(row.Action, row.RuleKey)

		// 3. 根据规则类型处理数据
		if rule.LuaEnable() {
			// 使用Lua脚本处理数据
			kvm := rowMap(row, rule, true) // 构建原始数据映射
			ls, err := luaengine.DoESOps(kvm, row.Action, rule)
			if err != nil {
				logs.Error("Lua 脚本执行失败!!! ,详情请参见日志")
				return errors.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
			}

			// 处理Lua脚本返回的多个ES操作
			for _, resp := range ls {
				logs.Infof("action: %s, Index: %s , Id:%s, value: %v", resp.Action, resp.Index, resp.Id, resp.Data)
				s.prepareBulk(resp.Action, resp.Index, rule.ElsType, resp.Id, resp.Data, bulk)
			}
		} else {
			// 使用内置规则处理数据
			kvm := rowMap(row, rule, false) // 构建格式化数据映射
			id := primaryKey(row, rule)     // 提取主键作为文档ID
			body := encodeValue(rule, kvm)  // 编码文档内容

			logs.Infof("action: %s, Index: %s , Id:%s, value: %v", row.Action, rule.ElsIndex, id, body)
			s.prepareBulk(row.Action, rule.ElsIndex, rule.ElsType, stringutil.ToString(id), body, bulk)
		}
	}

	// 4. 检查是否有待执行的操作
	if bulk.NumberOfActions() == 0 {
		return nil // 没有操作需要执行
	}

	// 5. 执行批量操作
	r, err := bulk.Do(context.Background())
	if err != nil {
		logs.Error(err.Error())
		return err // 批量操作执行失败
	}

	// 6. 检查批量操作结果
	if len(r.Failed()) > 0 {
		// 处理失败的操作
		for _, f := range r.Failed() {
			reason := f.Index + " " + f.Type + " " + f.Result

			// 对于删除操作，文档不存在是正常情况
			if f.Error == nil && "not_found" == f.Result {
				return nil
			}

			// 获取详细错误信息
			if f.Error != nil {
				reason = f.Error.Reason
			}

			logs.Error(reason)
			return errors.New(reason) // 返回第一个错误
		}
	}

	logs.Infof("处理完成 %d 条数据", len(rows))
	return nil // 所有数据处理成功
}

func (s *Elastic6Endpoint) Stock(rows []*model.RowRequest) int64 {
	if len(rows) == 0 {
		return 0
	}

	bulk := s.client.Bulk()
	for _, row := range rows {
		rule, _ := global.RuleIns(row.RuleKey)
		if rule.TableColumnSize != len(row.Row) {
			logs.Warnf("%s schema mismatching", row.RuleKey)
			continue
		}

		if rule.LuaEnable() {
			kvm := rowMap(row, rule, true)
			ls, err := luaengine.DoESOps(kvm, row.Action, rule)
			if err != nil {
				logs.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
				break
			}
			for _, resp := range ls {
				s.prepareBulk(resp.Action, resp.Index, rule.ElsType, resp.Id, resp.Data, bulk)
			}
		} else {
			kvm := rowMap(row, rule, false)
			id := primaryKey(row, rule)
			body := encodeValue(rule, kvm)
			s.prepareBulk(row.Action, rule.ElsIndex, rule.ElsType, stringutil.ToString(id), body, bulk)
		}
	}

	r, err := bulk.Do(context.Background())
	if err != nil {
		logs.Error(errors.ErrorStack(err))
		return 0
	}

	return int64(len(r.Succeeded()))
}

// prepareBulk 准备批量操作请求
// 根据操作类型创建相应的ES批量请求并添加到批量操作中
// action: 操作类型（INSERT/UPDATE/DELETE）
// index: ES索引名
// _type: ES文档类型（6.x版本需要）
// id: 文档ID
// doc: 文档内容（DELETE操作不需要）
// bulk: 批量操作服务对象
func (s *Elastic6Endpoint) prepareBulk(action, index, _type, id, doc string, bulk *elastic.BulkService) {
	switch action {
	case canal.InsertAction:
		// 创建索引请求（插入新文档）
		req := elastic.NewBulkIndexRequest().Index(index).Type(_type).Id(id).Doc(doc)
		bulk.Add(req)
	case canal.UpdateAction:
		// 创建更新请求（更新现有文档）
		req := elastic.NewBulkUpdateRequest().Index(index).Type(_type).Id(id).Doc(doc)
		bulk.Add(req)
	case canal.DeleteAction:
		// 创建删除请求（删除文档）
		req := elastic.NewBulkDeleteRequest().Index(index).Type(_type).Id(id)
		bulk.Add(req)
	}

	// 记录操作详情用于调试
	logs.Infof("index: %s, type:%s, action:%s, doc: %s", index, _type, action, doc)
}

// Close 关闭Elasticsearch连接
// 停止ES客户端，释放相关资源
func (s *Elastic6Endpoint) Close() {
	if s.client != nil {
		s.client.Stop() // 停止ES客户端
	}
}
