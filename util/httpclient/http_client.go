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

// Package httpclient 提供了一个功能丰富的HTTP客户端实现
// 支持GET、POST、PUT、DELETE等HTTP方法，具备超时、重试、参数传递等功能
// 主要特性：
// - 链式调用API设计，使用简单直观
// - 支持多种数据格式：JSON、表单、多部分表单
// - 内置重试机制，支持自定义重试条件
// - 支持超时设置和请求头管理
// - 提供默认客户端实例，开箱即用
package httpclient

import (
	"net/http" // 标准HTTP客户端
	"time"     // 时间处理

	"go.uber.org/zap" // 结构化日志库

	"go-mysql-transfer/util/logs" // 项目日志工具
)

// DefaultClient 默认的HTTP客户端实例
// 提供全局可用的HTTP客户端，无需手动创建即可使用
var DefaultClient = NewClient()

// HttpClient HTTP客户端结构体
// 封装了标准库的http.Client，提供更丰富的功能和更友好的API
type HttpClient struct {
	logger   *zap.Logger      // 日志记录器，用于记录请求日志和错误信息
	inner    *http.Client     // 底层的标准HTTP客户端
	criteria *requestCriteria // 全局请求条件配置（超时、重试等）
}

// NewClient 创建新的HTTP客户端实例
// 返回一个配置了默认参数的HttpClient实例
// 包含默认的日志记录器、标准HTTP客户端和空的请求条件
func NewClient() *HttpClient {
	return &HttpClient{
		logger:   logs.Logger(),        // 使用项目默认日志记录器
		inner:    &http.Client{},       // 创建标准HTTP客户端
		criteria: newRequestCriteria(), // 初始化请求条件配置
	}
}

// SetTimeout 设置HTTP请求超时时间
// timeout: 超时时间，单位为秒，必须大于0才会生效
// 返回客户端实例本身，支持链式调用
func (c *HttpClient) SetTimeout(timeout int) *HttpClient {
	if timeout > 0 {
		c.criteria.SetTimeout(timeout)                         // 设置请求条件中的超时时间
		c.inner.Timeout = time.Duration(timeout) * time.Second // 设置底层HTTP客户端的超时时间
	}
	return c
}

// SetLogger 设置自定义日志记录器
// logger: zap日志记录器实例
// 允许使用自定义的日志配置替换默认日志记录器
func (c *HttpClient) SetLogger(logger *zap.Logger) {
	c.logger = logger
}

// GetTimeout 获取当前设置的超时时间
// 返回超时时间（秒），如果未设置则返回0
func (c *HttpClient) GetTimeout() int {
	return c.criteria.timeout
}

// SetRetryCount 设置请求失败时的重试次数
// retryCount: 重试次数，0表示不重试
// 返回客户端实例本身，支持链式调用
func (c *HttpClient) SetRetryCount(retryCount int) *HttpClient {
	c.criteria.SetRetryCount(retryCount)
	return c
}

// GetRetryCount 获取当前设置的重试次数
// 返回重试次数，0表示不重试
func (c *HttpClient) GetRetryCount() int {
	return c.criteria.retryCount
}

// SetRetryInterval 设置重试间隔时间
// retryInterval: 重试间隔时间，单位为秒
// 返回客户端实例本身，支持链式调用
func (c *HttpClient) SetRetryInterval(retryInterval int) *HttpClient {
	c.criteria.SetRetryInterval(retryInterval)
	return c
}

// GetRetryInterval 获取当前设置的重试间隔时间
// 返回重试间隔时间（秒）
func (c *HttpClient) GetRetryInterval() int {
	return c.criteria.retryInterval
}

// AddRetryConditionFunc 添加自定义重试条件函数
// retryCondition: 重试条件判断函数，接收HTTP响应，返回是否需要重试
// 可以添加多个重试条件，任一条件满足即会触发重试
// 返回客户端实例本身，支持链式调用
func (c *HttpClient) AddRetryConditionFunc(retryCondition RetryConditionFunc) *HttpClient {
	c.criteria.AddRetryConditionFunc(retryCondition)
	return c
}

// SetTransport 设置HTTP传输层配置
// transport: HTTP传输层实现，用于确定HTTP请求的创建机制
// 如果为nil，将使用Go标准库的DefaultTransport
// 可用于配置代理、TLS设置、连接池等高级选项
// 返回客户端实例本身，支持链式调用
func (c *HttpClient) SetTransport(transport http.RoundTripper) *HttpClient {
	if transport != nil {
		c.inner.Transport = transport
	}
	return c
}

// AddHeader 添加单个HTTP请求头
// key: 请求头名称
// val: 请求头值
// 该请求头会应用到所有通过此客户端发送的请求
// 返回客户端实例本身，支持链式调用
func (c *HttpClient) AddHeader(key string, val string) *HttpClient {
	c.criteria.AddHeader(key, val)
	return c
}

// AddHeaders 批量添加HTTP请求头
// values: 请求头键值对映射
// 该方法会将所有请求头添加到客户端的全局请求头中
// 返回客户端实例本身，支持链式调用
func (c *HttpClient) AddHeaders(values H) *HttpClient {
	c.criteria.AddHeaders(values)
	return c
}

// GET 创建GET请求执行器
// url: 请求的URL地址
// 返回GET/DELETE请求执行器，支持添加查询参数、请求头等配置
// 使用链式调用方式配置请求参数，最后调用Do()执行请求
func (c *HttpClient) GET(url string) *GetOrDeleteExecutor {
	t := &GetOrDeleteExecutor{
		parameters: make(H), // 初始化查询参数映射
	}
	t.client = c                      // 设置客户端引用
	t.method = http.MethodGet         // 设置HTTP方法为GET
	t.addr = url                      // 设置请求URL
	t.criteria = newRequestCriteria() // 创建局部请求条件配置
	return t
}

// DELETE 创建DELETE请求执行器
// url: 请求的URL地址
// 返回GET/DELETE请求执行器，支持添加查询参数、请求头等配置
// DELETE请求通常用于删除资源
func (c *HttpClient) DELETE(url string) *GetOrDeleteExecutor {
	t := &GetOrDeleteExecutor{
		parameters: make(H), // 初始化查询参数映射
	}
	t.client = c                      // 设置客户端引用
	t.method = http.MethodDelete      // 设置HTTP方法为DELETE
	t.addr = url                      // 设置请求URL
	t.criteria = newRequestCriteria() // 创建局部请求条件配置
	return t
}

// POST 创建POST请求执行器
// url: 请求的URL地址
// 返回POST/PUT请求执行器，支持设置请求体、请求头等配置
// POST请求通常用于创建资源或提交数据
func (c *HttpClient) POST(url string) *PostOrPutExecutor {
	t := &PostOrPutExecutor{}         // 创建POST/PUT执行器
	t.client = c                      // 设置客户端引用
	t.method = http.MethodPost        // 设置HTTP方法为POST
	t.addr = url                      // 设置请求URL
	t.criteria = newRequestCriteria() // 创建局部请求条件配置
	return t
}

// PUT 创建PUT请求执行器
// url: 请求的URL地址
// 返回POST/PUT请求执行器，支持设置请求体、请求头等配置
// PUT请求通常用于更新资源
func (c *HttpClient) PUT(url string) *PostOrPutExecutor {
	t := &PostOrPutExecutor{}         // 创建POST/PUT执行器
	t.client = c                      // 设置客户端引用
	t.method = http.MethodPut         // 设置HTTP方法为PUT
	t.addr = url                      // 设置请求URL
	t.criteria = newRequestCriteria() // 创建局部请求条件配置
	return t
}
