// Package httpclient 的请求执行器模块
// 实现了HTTP请求的具体执行逻辑，包括GET、POST、PUT、DELETE等方法
// 支持多种数据格式：表单、JSON、多部分表单（文件上传）
// 提供重试机制、超时控制、响应处理等功能
package httpclient

import (
	"bytes"          // 字节缓冲区操作
	"encoding/json"  // JSON编码解码
	"io"             // IO操作接口
	"io/ioutil"      // IO工具函数（已废弃，但项目中仍在使用）
	"mime/multipart" // 多部分表单处理
	"net/http"       // HTTP客户端和服务器
	"net/url"        // URL解析和构建
	"os"             // 操作系统接口
	"strings"        // 字符串操作
	"time"           // 时间处理

	"github.com/pkg/errors" // 错误处理增强库

	"go-mysql-transfer/util/stringutil" // 项目字符串工具
)

// 内容类型常量定义
const (
	_contentTypeForm = 1 // 表单类型：application/x-www-form-urlencoded
	_contentTypeJson = 2 // JSON类型：application/json
)

// FormFile 表示要上传的文件路径
// 在多部分表单中使用，用于标识文件字段
// 例如：FormFile("/path/to/file.jpg")
type FormFile string

// executor 请求执行器基础结构体
// 包含执行HTTP请求所需的基本信息和配置
type executor struct {
	client       *HttpClient      // HTTP客户端引用
	criteria     *requestCriteria // 请求条件配置（超时、重试等）
	addr         string           // 请求URL地址
	method       string           // HTTP方法（GET、POST、PUT、DELETE）
	expectStatus int              // 期望的HTTP状态码，0表示不检查
}

// GetOrDeleteExecutor GET和DELETE请求执行器
// 继承基础执行器，添加查询参数支持
// GET和DELETE请求通常不包含请求体，主要通过URL参数传递数据
type GetOrDeleteExecutor struct {
	executor     // 嵌入基础执行器
	parameters H // 查询参数映射，会被编码到URL中
}

// PostOrPutExecutor POST和PUT请求执行器
// 继承基础执行器，添加请求体支持
// POST和PUT请求通常包含请求体，支持多种数据格式
type PostOrPutExecutor struct {
	executor                // 嵌入基础执行器
	body        interface{} // 请求体数据，支持多种类型
	contentType int         // 内容类型标识（表单或JSON）
}

// overrideCriteria 合并全局和局部请求条件配置
// 全局配置作为默认值，局部配置具有更高优先级
// 这种设计允许在客户端级别设置默认配置，在请求级别进行个性化定制
func (s *executor) overrideCriteria() {
	global := s.client.criteria // 获取客户端全局配置
	local := s.criteria         // 获取当前请求的局部配置

	// 如果局部未设置重试次数，使用全局配置
	if local.retryCount == 0 {
		local.retryCount = global.retryCount
	}

	// 如果局部未设置重试间隔，使用全局配置
	if local.retryInterval == 0 {
		local.retryInterval = global.retryInterval
	}

	// 合并重试条件函数列表
	// 全局和局部的重试条件都会生效
	for _, retryCondition := range global.retryConditions {
		local.retryConditions = append(local.retryConditions, retryCondition)
	}

	// 合并请求头，局部请求头优先级更高
	// 如果局部没有设置某个请求头，则使用全局设置
	for k, v := range global.headers {
		if _, exist := local.headers[k]; !exist { // 局部未设置该请求头
			local.headers[k] = v // 使用全局请求头
		}
	}
}

// execute 执行HTTP请求的核心方法
// 负责发送请求、处理重试、记录日志等核心逻辑
// request: 已构建好的HTTP请求对象
// 返回HTTP响应和可能的错误
func (s *executor) execute(request *http.Request) (*http.Response, error) {
	// 合并全局和局部配置
	s.overrideCriteria()

	// 设置请求头
	// 遍历所有配置的请求头，添加到HTTP请求中
	for k, v := range s.criteria.headers {
		request.Header.Add(k, stringutil.ToString(v)) // 将值转换为字符串
	}

	// 记录请求开始时间，用于计算请求耗时
	startTime := time.Now().UnixNano()

	// 发送HTTP请求
	res, err := s.client.inner.Do(request)

	// 计算请求耗时（毫秒）
	latency := (time.Now().UnixNano() - startTime) / int64(time.Millisecond)

	// 如果请求成功，记录成功日志
	if nil == err {
		s.client.logger.Sugar().Infof("请求成功, %s | %s | %d | %d(毫秒)",
			request.Method, request.URL.String(), res.StatusCode, latency)
	}

	// 重试机制处理
	if s.criteria.retryCount > 0 && s.criteria.needRetry(res) {
		// 执行重试循环
		for i := 0; i < s.criteria.retryCount; i++ {
			s.client.logger.Sugar().Infof("第%d次重试： %s | %s )",
				i+1, request.Method, request.URL.String())

			// 重新发送请求
			res, err = s.client.inner.Do(request)
			if err != nil {
				s.client.logger.Error(err.Error()) // 记录重试过程中的错误
			}

			// 检查是否还需要继续重试
			if !s.criteria.needRetry(res) || (i+1) == s.criteria.retryCount {
				return res, err // 不需要重试或达到最大重试次数，返回结果
			}

			// 等待重试间隔时间
			<-time.After(time.Duration(s.criteria.retryInterval) * time.Second)
		}
	}

	// 检查期望状态码
	// 如果设置了期望状态码且实际状态码不匹配，返回错误
	if s.expectStatus != 0 && s.expectStatus != res.StatusCode {
		defer res.Body.Close() // 确保关闭响应体
		return nil, errors.Errorf("Response status code : %d (%s)",
			res.StatusCode, http.StatusText(res.StatusCode))
	}

	return res, err
}

// responseAsString 将HTTP响应转换为字符串
// response: HTTP响应对象
// 返回响应体的字符串内容和可能的错误
// 该方法会自动关闭响应体，防止资源泄露
func (s *executor) responseAsString(response *http.Response) (string, error) {
	defer response.Body.Close() // 确保响应体被关闭

	// 读取响应体的所有数据
	if data, err := ioutil.ReadAll(response.Body); err == nil {
		return string(data), nil // 转换为字符串并返回
	}

	return "", nil // 读取失败时返回空字符串
}

// responseAsEntity 将HTTP响应转换为响应实体对象
// response: HTTP响应对象
// 返回包含状态码和数据的RespondEntity对象和可能的错误
// RespondEntity提供了更丰富的响应处理方法
func (s *executor) responseAsEntity(response *http.Response) (*RespondEntity, error) {
	defer response.Body.Close() // 确保响应体被关闭

	// 读取响应体的所有数据
	data, err := ioutil.ReadAll(response.Body)
	if nil != err {
		return nil, err // 读取失败时返回错误
	}

	// 创建并返回响应实体对象
	return &RespondEntity{
		statusCode: response.StatusCode, // HTTP状态码
		data:       data,                // 响应体数据
	}, nil
}

// AddHeader 为GET/DELETE请求添加单个HTTP请求头
// name: 请求头名称（如"Authorization", "Content-Type"等）
// value: 请求头值，支持任意类型，会在发送时转换为字符串
// 返回执行器本身，支持链式调用
func (r *GetOrDeleteExecutor) AddHeader(name string, value interface{}) *GetOrDeleteExecutor {
	r.criteria.AddHeader(name, value)
	return r
}

// SetHeaders 为GET/DELETE请求批量设置HTTP请求头
// values: 请求头键值对映射
// 返回执行器本身，支持链式调用
func (r *GetOrDeleteExecutor) SetHeaders(values H) *GetOrDeleteExecutor {
	r.criteria.AddHeaders(values)
	return r
}

// SetRetryCount 设置GET/DELETE请求的重试次数
// _retryCount: 重试次数，0表示不重试
// 返回执行器本身，支持链式调用
func (r *GetOrDeleteExecutor) SetRetryCount(_retryCount int) *GetOrDeleteExecutor {
	r.criteria.SetRetryCount(_retryCount)
	return r
}

// SetRetryInterval 设置GET/DELETE请求的重试间隔时间
// _retryInterval: 重试间隔时间，单位为秒
// 返回执行器本身，支持链式调用
func (r *GetOrDeleteExecutor) SetRetryInterval(_retryInterval int) *GetOrDeleteExecutor {
	r.criteria.SetRetryInterval(_retryInterval)
	return r
}

// AddRetryConditionFunc 为GET/DELETE请求添加重试条件函数
// _retryCondition: 重试条件判断函数
// 返回执行器本身，支持链式调用
func (r *GetOrDeleteExecutor) AddRetryConditionFunc(_retryCondition RetryConditionFunc) *GetOrDeleteExecutor {
	r.criteria.AddRetryConditionFunc(_retryCondition)
	return r
}

// AddParameter 为GET/DELETE请求添加单个查询参数
// name: 参数名称
// value: 参数值，支持任意类型，会在发送时转换为字符串并进行URL编码
// 返回执行器本身，支持链式调用
func (r *GetOrDeleteExecutor) AddParameter(name string, value interface{}) *GetOrDeleteExecutor {
	r.parameters[name] = value
	return r
}

// AddParameters 为GET/DELETE请求批量添加查询参数
// values: 参数键值对映射
// 所有参数会被编码到URL的查询字符串中
// 返回执行器本身，支持链式调用
func (r *GetOrDeleteExecutor) AddParameters(values H) *GetOrDeleteExecutor {
	for k, v := range values {
		r.parameters[k] = v // 将每个参数添加到参数映射中
	}
	return r
}

// SetExpectStatus 设置GET/DELETE请求的期望HTTP状态码
// expect: 期望的HTTP状态码，如果实际状态码不匹配会返回错误
// 0表示不检查状态码
// 返回执行器本身，支持链式调用
func (r *GetOrDeleteExecutor) SetExpectStatus(expect int) *GetOrDeleteExecutor {
	r.expectStatus = expect
	return r
}

// Do 执行GET/DELETE请求
// 构建完整的请求URL（包含查询参数），创建HTTP请求并执行
// 返回原始的HTTP响应对象和可能的错误
func (r *GetOrDeleteExecutor) Do() (*http.Response, error) {
	// 构建URL查询参数
	values := make(url.Values)
	for k, v := range r.parameters {
		values.Set(k, stringutil.ToString(v)) // 将参数值转换为字符串
	}

	// 将查询参数编码到URL中
	url := stringutil.UrlValuesToQueryString(r.addr, values)

	// 创建HTTP请求对象
	// GET和DELETE请求通常没有请求体，所以body参数为nil
	req, err := http.NewRequest(r.method, url, nil)
	if nil != err {
		return nil, err // 创建请求失败
	}

	// 执行请求
	return r.execute(req)
}

// DoForString 执行GET/DELETE请求并返回字符串响应
// 这是一个便捷方法，自动处理响应体的读取和转换
// 返回响应体的字符串内容和可能的错误
func (r *GetOrDeleteExecutor) DoForString() (string, error) {
	res, err := r.Do() // 执行请求
	if nil != err {
		return "", err // 请求失败时返回空字符串和错误
	}
	return r.responseAsString(res) // 将响应转换为字符串
}

// DoForEntity 执行GET/DELETE请求并返回响应实体对象
// 这是一个便捷方法，返回包含状态码和数据的RespondEntity对象
// 返回响应实体对象和可能的错误
func (r *GetOrDeleteExecutor) DoForEntity() (*RespondEntity, error) {
	res, err := r.Do() // 执行请求
	if nil != err {
		return nil, err // 请求失败时返回nil和错误
	}
	return r.responseAsEntity(res) // 将响应转换为实体对象
}

// 设置请求头
func (r *PostOrPutExecutor) AddHeader(name string, value interface{}) *PostOrPutExecutor {
	r.criteria.AddHeader(name, value)
	return r
}

// 设置请求头
func (r *PostOrPutExecutor) SetHeaders(values H) *PostOrPutExecutor {
	r.criteria.AddHeaders(values)
	return r
}

// 设置重试次数
func (r *PostOrPutExecutor) SetRetryCount(_retryCount int) *PostOrPutExecutor {
	r.criteria.SetRetryCount(_retryCount)
	return r
}

// 设置重试间隔时间，单位为秒
func (r *PostOrPutExecutor) SetRetryInterval(_retryInterval int) *PostOrPutExecutor {
	r.criteria.SetRetryInterval(_retryInterval)
	return r
}

// 添加重试条件
func (r *PostOrPutExecutor) AddRetryConditionFunc(_retryCondition RetryConditionFunc) *PostOrPutExecutor {
	r.criteria.AddRetryConditionFunc(_retryCondition)
	return r
}

// 设置预期请求状态
func (r *PostOrPutExecutor) SetExpectStatus(expect int) *PostOrPutExecutor {
	r.expectStatus = expect
	return r
}

func (r *PostOrPutExecutor) SetBodyAsForm(body H) *PostOrPutExecutor {
	r.contentType = _contentTypeForm
	r.body = body
	return r
}

// 设置请求的contentType 为：
// "application/json"
func (r *PostOrPutExecutor) SetBodyAsJson(body interface{}) *PostOrPutExecutor {
	r.contentType = _contentTypeJson
	r.body = body
	return r
}

// 执行请求
func (r *PostOrPutExecutor) Do() (*http.Response, error) {
	if _contentTypeForm == r.contentType {
		return r.doFormRequest()
	}

	var data []byte
	switch r.body.(type) {
	case string:
		ft := r.body.(string)
		data = []byte(ft)
	case []byte:
		data = r.body.([]byte)
	default:
		temp, err := json.Marshal(r.body)
		if err != nil {
			return nil, err
		}
		data = temp
	}

	req, err := http.NewRequest(r.method, r.addr, bytes.NewReader(data))
	if nil != err {
		return nil, err
	}
	req.Header.Add("Content-Type", "application/json")
	return r.execute(req)
}

func (r *PostOrPutExecutor) DoForString() (string, error) {
	res, err := r.Do()
	if nil != err {
		return "", err
	}
	return r.responseAsString(res)
}

func (r *PostOrPutExecutor) DoForEntity() (*RespondEntity, error) {
	res, err := r.Do()
	if nil != err {
		return nil, err
	}
	return r.responseAsEntity(res)
}

func (r *PostOrPutExecutor) doFormRequest() (*http.Response, error) {
	data := r.body.(H)
	isMultipart := false
	values := make(url.Values)
	for k, v := range data {
		if _, ok := v.(FormFile); ok {
			isMultipart = true
			values = nil
			break
		}
		values.Set(k, stringutil.ToString(v))
	}

	if !isMultipart {
		req, err := http.NewRequest(r.method, r.addr, strings.NewReader(values.Encode()))
		if nil != err {
			return nil, err
		}
		req.Header.Add("Content-Type", "application/x-www-form-urlencoded")
		return r.execute(req)
	}

	var err error
	bodyBuffer := &bytes.Buffer{}
	bodyWriter := multipart.NewWriter(bodyBuffer)

	var closings []*os.File
	defer func() {
		for _, closing := range closings {
			closing.Close()
		}
	}()

	for k, v := range data {
		switch v.(type) {
		case FormFile:
			vv := v.(FormFile)
			var fw io.Writer
			fw, err = bodyWriter.CreateFormFile(k, string(vv))
			if err != nil {
				break
			}

			var file *os.File
			file, err = os.Open(string(vv))
			if err != nil {
				break
			}
			closings = append(closings, file)

			_, err = io.Copy(fw, file)
			if err != nil {
				break
			}
		default:
			if err := bodyWriter.WriteField(k, stringutil.ToString(v)); err != nil {
				return nil, err
			}
		}
	}

	if err != nil {
		return nil, err
	}

	bodyWriter.Close()

	var req *http.Request
	req, err = http.NewRequest(r.method, r.addr, bodyBuffer)
	if nil != err {
		return nil, err
	}

	req.Header.Add("Content-Type", bodyWriter.FormDataContentType())
	return r.execute(req)
}
