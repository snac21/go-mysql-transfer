// Package httpclient 的请求条件和参数定义模块
// 定义了HTTP请求的各种配置参数和重试条件
package httpclient

import "net/http" // 标准HTTP库

// H 是HTTP参数的类型别名
// 用于表示键值对参数，如查询参数、请求头、表单数据等
// 使用interface{}作为值类型，支持字符串、数字、布尔值等多种数据类型
type H map[string]interface{}

// RetryConditionFunc 重试条件判断函数类型
// 接收HTTP响应作为参数，返回布尔值表示是否需要重试
// 可以根据响应状态码、响应内容等条件来决定是否重试
// 例如：网络错误、5xx服务器错误、特定的业务错误码等
type RetryConditionFunc func(*http.Response) bool

// requestCriteria 请求条件配置结构体
// 包含了HTTP请求的各种配置参数，如超时、重试、请求头等
type requestCriteria struct {
	timeout         int                  // 请求超时时间（秒）
	retryCount      int                  // 重试次数，0表示不重试
	retryInterval   int                  // 重试间隔时间（秒）
	retryConditions []RetryConditionFunc // 重试条件函数列表，任一条件满足即重试
	headers         H                    // 全局请求头映射
}

// newRequestCriteria 创建新的请求条件配置实例
// 返回一个初始化的requestCriteria实例，包含空的请求头映射
func newRequestCriteria() *requestCriteria {
	return &requestCriteria{
		headers: make(H), // 初始化空的请求头映射
	}
}

// AddHeader 添加单个HTTP请求头
// name: 请求头名称（如"Content-Type", "Authorization"等）
// value: 请求头值，支持任意类型，会在发送时转换为字符串
func (c *requestCriteria) AddHeader(name string, value interface{}) {
	c.headers[name] = value
}

// AddHeaders 批量添加HTTP请求头
// values: 请求头键值对映射
// 遍历映射中的所有键值对，逐个添加到请求头中
func (c *requestCriteria) AddHeaders(values H) {
	for k, v := range values {
		c.headers[k] = v // 将每个键值对添加到请求头映射中
	}
}

// SetTimeout 设置HTTP请求超时时间
// _timeout: 超时时间，单位为秒
// 超时时间会应用到整个HTTP请求过程，包括连接建立、数据传输等
func (c *requestCriteria) SetTimeout(_timeout int) {
	c.timeout = _timeout
}

// SetRetryCount 设置请求失败时的重试次数
// _retryCount: 重试次数，0表示不重试，大于0表示最大重试次数
func (c *requestCriteria) SetRetryCount(_retryCount int) {
	c.retryCount = _retryCount
}

// SetRetryInterval 设置重试间隔时间
// _retryInterval: 重试间隔时间，单位为秒
// 在每次重试之间会等待指定的时间间隔
func (c *requestCriteria) SetRetryInterval(_retryInterval int) {
	c.retryInterval = _retryInterval
}

// AddRetryConditionFunc 添加重试条件判断函数
// _retryCondition: 重试条件函数，用于判断是否需要重试
// 可以添加多个重试条件函数，任一函数返回true即会触发重试
// 如果传入nil则忽略，不会添加到条件列表中
func (c *requestCriteria) AddRetryConditionFunc(_retryCondition RetryConditionFunc) {
	if _retryCondition != nil {
		c.retryConditions = append(c.retryConditions, _retryCondition)
	}
}

// needRetry 判断是否需要重试请求
// res: HTTP响应对象
// 返回true表示需要重试，false表示不需要重试
// 遍历所有重试条件函数，任一函数返回true即表示需要重试
func (c *requestCriteria) needRetry(res *http.Response) bool {
	for _, condition := range c.retryConditions {
		if condition(res) { // 如果任一重试条件满足
			return true // 返回需要重试
		}
	}
	return false // 所有条件都不满足，不需要重试
}
