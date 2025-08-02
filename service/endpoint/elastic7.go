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

// Package endpoint Elasticsearch 7.x端点实现
// 提供与Elasticsearch 7.x集群的数据同步功能
// 支持索引映射管理、批量操作、Lua脚本自定义处理等特性
package endpoint

import (
	"context" // 上下文管理，用于控制请求生命周期
	"log"     // 标准日志

	// 同步原语（未使用的retryLock字段）
	"github.com/go-mysql-org/go-mysql/canal" // Canal binlog解析
	"github.com/go-mysql-org/go-mysql/mysql" // MySQL协议和类型
	"github.com/juju/errors"                 // 错误处理增强
	"github.com/olivere/elastic/v7"          // Elasticsearch 7.x客户端库

	"go-mysql-transfer/global"            // 全局配置和规则
	"go-mysql-transfer/metrics"           // 监控指标
	"go-mysql-transfer/model"             // 数据模型
	"go-mysql-transfer/service/luaengine" // Lua脚本引擎
	"go-mysql-transfer/util/logagent"     // 日志代理
	"go-mysql-transfer/util/logs"         // 日志工具
	"go-mysql-transfer/util/stringutil"   // 字符串工具
)

// Elastic7Endpoint Elasticsearch 7.x端点实现结构体
// 负责将MySQL数据变更同步到Elasticsearch 7.x集群
// 支持索引映射自动管理、批量操作优化等功能
type Elastic7Endpoint struct {
	first  string          // 第一个ES节点地址，用于健康检查
	hosts  []string        // ES集群节点地址列表
	client *elastic.Client // Elasticsearch 7.x客户端实例
}

// newElastic7Endpoint 创建Elasticsearch 7.x端点实例
// 解析配置中的ES集群地址并初始化端点结构体
// 返回初始化的Elastic7Endpoint实例
func newElastic7Endpoint() *Elastic7Endpoint {
	hosts := elsHosts(global.Cfg().ElsAddr) // 解析ES集群地址列表
	r := &Elastic7Endpoint{}                // 创建端点实例
	r.hosts = hosts                         // 设置集群节点地址
	r.first = hosts[0]                      // 设置第一个节点地址用于健康检查
	return r                                // 返回初始化的实例
}

// Connect 连接到Elasticsearch 7.x集群
// 配置ES客户端选项，建立连接并初始化索引映射
// 返回连接过程中可能出现的错误
func (s *Elastic7Endpoint) Connect() error {
	var options []elastic.ClientOptionFunc // ES客户端配置选项列表

	// 设置错误日志代理，用于记录ES操作错误
	options = append(options, elastic.SetErrorLog(logagent.NewElsLoggerAgent()))

	// 设置ES集群节点URL列表
	options = append(options, elastic.SetURL(s.hosts...))

	// 如果配置了用户名和密码，启用基本认证
	if global.Cfg().ElsUser != "" && global.Cfg().ElsPassword != "" {
		options = append(options, elastic.SetBasicAuth(global.Cfg().ElsUser, global.Cfg().ElsPassword))
	}

	// 创建ES客户端实例
	client, err := elastic.NewClient(options...)
	if err != nil {
		return err // 客户端创建失败
	}

	s.client = client       // 保存客户端实例
	return s.indexMapping() // 初始化索引映射
}

// indexMapping 初始化所有索引的映射配置
// 遍历所有同步规则，为每个规则对应的索引创建或更新映射
// 返回映射初始化过程中的错误
func (s *Elastic7Endpoint) indexMapping() error {
	// 遍历所有同步规则
	for _, rule := range global.RuleInsList() {
		// 检查索引是否已存在
		exists, err := s.client.IndexExists(rule.ElsIndex).Do(context.Background())
		if err != nil {
			return err // 检查索引存在性失败
		}

		if exists {
			// 索引已存在，更新映射配置
			err = s.updateIndexMapping(rule)
		} else {
			// 索引不存在，创建新索引和映射
			err = s.insertIndexMapping(rule)
		}

		if err != nil {
			return err // 映射操作失败
		}
	}

	return nil // 所有索引映射初始化成功
}

func (s *Elastic7Endpoint) insertIndexMapping(rule *global.Rule) error {
	var properties map[string]interface{}
	if rule.LuaEnable() {
		properties = buildPropertiesByMappings(rule)
	} else {
		properties = buildPropertiesByRule(rule)
	}

	mapping := map[string]interface{}{
		"mappings": map[string]interface{}{
			"properties": properties,
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

	logs.Infof("create index: %s ,mappings: %s", rule.ElsIndex, body)

	return nil
}

func (s *Elastic7Endpoint) updateIndexMapping(rule *global.Rule) error {
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

	if retMaps["properties"] == nil {
		return nil
	}
	retPros := retMaps["properties"].(map[string]interface{})

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
		ret, err := s.client.PutMapping().Index(rule.ElsIndex).BodyString(doc).Do(context.Background())
		if err != nil {
			return err
		}
		if !ret.Acknowledged {
			return errors.Errorf("update index %s err", rule.ElsIndex)
		}

		logs.Infof("update index: %s ,properties: %s", rule.ElsIndex, doc)
	}

	return nil
}

// Ping 检查Elasticsearch集群连接状态
// 依次尝试ping集群中的各个节点，确保至少有一个节点可用
// 返回连接检查的结果，nil表示连接正常
func (s *Elastic7Endpoint) Ping() error {
	// 首先尝试ping第一个节点
	if _, _, err := s.client.Ping(s.first).Do(context.Background()); err == nil {
		return nil // 第一个节点可用
	}

	// 如果第一个节点不可用，尝试其他节点
	for _, host := range s.hosts {
		if _, _, err := s.client.Ping(host).Do(context.Background()); err == nil {
			return nil // 找到可用节点
		}
	}

	// 所有节点都不可用
	return errors.New("all elasticsearch nodes are unavailable")
}

// Consume 消费增量数据，将MySQL变更同步到Elasticsearch
// 处理binlog解析后的行变更请求，转换为ES批量操作并执行
// from: MySQL binlog位置信息
// rows: 行变更请求列表
// 返回数据消费过程中的错误
func (s *Elastic7Endpoint) Consume(from mysql.Position, rows []*model.RowRequest) error {
	bulk := s.client.Bulk() // 创建ES批量操作对象

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
			kvm := rowMap(row, rule, true)                      // 将行数据转换为键值映射
			ls, err := luaengine.DoESOps(kvm, row.Action, rule) // 执行Lua脚本
			if err != nil {
				log.Println("Lua 脚本执行失败!!! ,详情请参见日志")
				return errors.Errorf("lua 脚本执行失败 : %s ", errors.ErrorStack(err))
			}

			// 处理Lua脚本生成的所有ES操作
			for _, resp := range ls {
				logs.Infof("action: %s, Index: %s , Id:%s, value: %v",
					resp.Action, resp.Index, resp.Id, resp.Data)
				// 将操作添加到批量请求中
				s.prepareBulk(resp.Action, resp.Index, resp.Id, resp.Data, bulk)
			}
		} else {
			// 使用内置规则处理数据
			kvm := rowMap(row, rule, false) // 将行数据转换为键值映射
			id := primaryKey(row, rule)     // 获取文档ID（通常是主键）
			body := encodeValue(rule, kvm)  // 编码文档内容

			logs.Infof("action: %s, Index: %s , Id:%s, value: %v",
				row.Action, rule.ElsIndex, id, body)
			// 将操作添加到批量请求中
			s.prepareBulk(row.Action, rule.ElsIndex, stringutil.ToString(id), body, bulk)
		}
	}

	// 如果没有操作需要执行，直接返回
	if bulk.NumberOfActions() == 0 {
		return nil
	}

	// 执行批量操作
	r, err := bulk.Do(context.Background())
	if err != nil {
		return err // 批量操作执行失败
	}

	// 检查是否有失败的操作
	if len(r.Failed()) > 0 {
		for _, f := range r.Failed() {
			// 构建错误原因描述
			reason := f.Index + " " + f.Type + " " + f.Result

			// 如果是删除操作且文档不存在，不视为错误
			if f.Error == nil && f.Result == "not_found" {
				return nil
			}

			// 如果有具体的错误信息，使用错误信息
			if f.Error != nil {
				reason = f.Error.Reason
			}

			log.Println(reason)
			return errors.New(reason) // 返回第一个错误
		}
	}

	logs.Infof("处理完成 %d 条数据", len(rows))
	return nil // 处理成功
}

func (s *Elastic7Endpoint) Stock(rows []*model.RowRequest) int64 {
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
				s.prepareBulk(resp.Action, resp.Index, resp.Id, resp.Data, bulk)
			}
		} else {
			kvm := rowMap(row, rule, false)
			id := primaryKey(row, rule)
			body := encodeValue(rule, kvm)
			s.prepareBulk(row.Action, rule.ElsIndex, stringutil.ToString(id), body, bulk)
		}
	}

	r, err := bulk.Do(context.Background())
	if err != nil {
		logs.Error(errors.ErrorStack(err))
		return 0
	}

	if len(r.Failed()) > 0 {
		for _, f := range r.Failed() {
			logs.Error(f.Error.Reason)
		}
	}

	return int64(len(r.Succeeded()))
}

// prepareBulk 根据操作类型准备批量请求
// 将不同的数据库操作转换为对应的ES批量操作请求
// action: 操作类型（INSERT/UPDATE/DELETE）
// index: ES索引名称
// id: 文档ID
// doc: 文档内容（JSON字符串）
// bulk: ES批量操作服务对象
func (s *Elastic7Endpoint) prepareBulk(action, index, id, doc string, bulk *elastic.BulkService) {
	// 根据操作类型创建相应的批量请求
	switch action {
	case canal.InsertAction:
		// INSERT操作：创建索引文档请求
		req := elastic.NewBulkIndexRequest().Index(index).Id(id).Doc(doc)
		bulk.Add(req) // 添加到批量操作中

	case canal.UpdateAction:
		// UPDATE操作：创建更新文档请求
		req := elastic.NewBulkUpdateRequest().Index(index).Id(id).Doc(doc)
		bulk.Add(req) // 添加到批量操作中

	case canal.DeleteAction:
		// DELETE操作：创建删除文档请求（不需要文档内容）
		req := elastic.NewBulkDeleteRequest().Index(index).Id(id)
		bulk.Add(req) // 添加到批量操作中
	}

	// 记录操作详情日志
	logs.Infof("index: %s, doc: %s", index, doc)
}

// Close 关闭Elasticsearch连接
// 停止ES客户端，释放相关资源
func (s *Elastic7Endpoint) Close() {
	if s.client != nil {
		s.client.Stop() // 停止ES客户端
	}
}
