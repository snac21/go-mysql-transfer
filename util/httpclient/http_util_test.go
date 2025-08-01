// Package httpclient 提供HTTP客户端功能的测试用例
// 该包测试了HTTP客户端的各种功能，包括GET、POST、PUT、DELETE请求，
// 以及超时、重试、参数传递、表单提交、JSON提交、文件上传等功能
package httpclient

import (
	"fmt"      // 用于格式化输出
	"io"       // 用于IO操作，特别是读取响应体
	"net/http" // HTTP客户端和服务器实现
	"testing"  // Go语言测试框架
)

// TestHttpClientGet 测试HTTP GET请求的基本功能
// 使用默认客户端向httpbin.org发送GET请求，验证请求是否成功
func TestHttpClientGet(t *testing.T) {
	// 使用默认HTTP客户端发送GET请求到测试服务器
	res, err := DefaultClient.GET("http://httpbin.org/get").Do()
	if err != nil {
		t.Error("get failed", err) // 如果请求失败，记录错误
	}

	// 确保响应体在函数结束时关闭，防止资源泄露
	defer res.Body.Close()

	// 验证HTTP状态码是否为200（成功）
	if res.StatusCode != 200 {
		t.Error("Status Code not 200")
	}
}

// TestHttpClientTimeout 测试HTTP客户端的超时功能
// 设置10秒超时时间，向本地不存在的服务发送请求
func TestHttpClientTimeout(t *testing.T) {
	// 设置超时时间为10秒，向本地8801端口发送GET请求
	// 这个端口通常不会有服务响应，用于测试超时机制
	res, err := DefaultClient.SetTimeout(10).GET("http://127.0.0.1:8801/zombie").Do()
	if err != nil {
		t.Error("get failed", err) // 预期会因为连接失败而出错
	}

	// 如果有响应，确保关闭响应体
	defer res.Body.Close()

	// 检查状态码（在实际场景中，这个请求可能会失败）
	if res.StatusCode != 200 {
		t.Error("Status Code not 200")
	}
}

// TestHttpClientGetForString 测试GET请求并直接获取字符串响应
// 使用DoForString方法直接获取响应内容的字符串形式
func TestHttpClientGetForString(t *testing.T) {
	// 发送GET请求并直接获取响应内容的字符串形式
	// DoForString方法会自动处理响应体的读取和关闭
	str, err := DefaultClient.GET("http://httpbin.org/get").DoForString()
	if err != nil {
		t.Error("get failed", err) // 记录请求失败错误
	}

	// 打印响应内容（用于调试和验证）
	println(str)
}

// TestHttpClientGetWithParameters 测试带查询参数的GET请求
// 演示如何添加多个查询参数，参数会自动编码到URL中
func TestHttpClientGetWithParameters(t *testing.T) {
	// 实际请求地址：http://httpbin.org/get?token=12596358412&name=wf&age=18
	// 使用链式调用添加多个查询参数
	str, err := DefaultClient.GET("http://httpbin.org/get").
		AddParameters(H{ // 第一组参数
			"token": "12596358412", // 添加token参数
		}).
		AddParameters(H{ // 第二组参数
			"name": "wf", // 添加name参数
			"age":  18,   // 添加age参数（会自动转换为字符串）
		}).
		DoForString() // 执行请求并获取字符串响应

	if err != nil {
		t.Error("get failed", err) // 记录请求失败错误
	}

	// 打印响应内容，可以看到服务器接收到的参数
	println(str)
}

// retryCondition 定义重试条件的函数
// 该函数决定在什么情况下需要重试HTTP请求
func retryCondition(res *http.Response) bool {
	// 如果响应为空（网络错误等），需要重试
	if res == nil {
		return true
	}
	// 如果HTTP状态码不是200（成功），需要重试
	if res.StatusCode != http.StatusOK {
		return true
	}

	// 注意：这里总是返回true，意味着总是会重试
	// 在实际使用中，应该根据具体业务逻辑来决定重试条件
	return true
}

// TestHttpClientRetry 测试HTTP客户端的重试机制
// 向不存在的域名发送请求，触发重试逻辑
func TestHttpClientRetry(t *testing.T) {
	// 创建新的HTTP客户端并配置重试参数
	NewClient().
		SetRetryCount(3).                      // 设置重试次数为3次
		SetRetryInterval(5).                   // 设置重试间隔为5秒
		AddRetryConditionFunc(retryCondition). // 添加自定义重试条件函数
		GET("http://test.org/get").            // 向不存在的域名发送GET请求
		Do()                                   // 执行请求

	// 预期的日志输出示例：
	// {"level":"error","msg":"Get \"http://test.org/get\": dial tcp: lookup test.org: no such host"}
	// {"level":"info","msg":"第1次重试： GET | http://test.org/get )"}
	// {"level":"error","msg":"Get \"http://test.org/get\": dial tcp: lookup test.org: no such host"}
	// {"level":"info","msg":"第2次重试： GET | http://test.org/get )"}
	// {"level":"error","msg":"Get \"http://test.org/get\": dial tcp: lookup test.org: no such host"}
	// {"level":"info","msg":"第3次重试： GET | http://test.org/get )"}
	// {"level":"error","msg":"Get \"http://test.org/get\": dial tcp: lookup test.org: no such host"}
}

// TestHttpClientPostForm 测试POST表单提交功能
// 使用application/x-www-form-urlencoded格式提交表单数据
func TestHttpClientPostForm(t *testing.T) {
	// Content-Type 会自动设置为: application/x-www-form-urlencoded
	// 发送POST请求，提交表单数据
	res, err := DefaultClient.POST("http://httpbin.org/post").
		SetBodyAsForm(H{ // 设置表单数据
			"name": "wf", // 姓名字段
			"age":  18,   // 年龄字段（会自动转换为字符串）
		}).Do() // 执行请求

	if err != nil {
		t.Error("post failed", err) // 记录POST请求失败错误
	}

	// 确保响应体在函数结束时关闭
	defer res.Body.Close()

	// 验证HTTP状态码
	if res.StatusCode != 200 {
		t.Error("Status Code not 200")
	}

	// 读取响应体内容
	data, err := io.ReadAll(res.Body)
	if nil != err {
		t.Error("read failed", err) // 记录读取响应失败错误
	}

	// 打印响应内容，可以看到服务器接收到的表单数据
	fmt.Println(string(data))
}

// Person 定义用于JSON序列化测试的结构体
// 用于演示如何将Go结构体作为JSON数据发送
type Person struct {
	Name string // 姓名字段
	Age  int    // 年龄字段
}

// TestHttpClientPostJson 测试POST JSON数据提交功能
// 使用application/json格式提交JSON数据，支持多种数据类型
func TestHttpClientPostJson(t *testing.T) {
	// Content-Type 会自动设置为: application/json
	// 方式1: 使用map提交JSON数据
	res, err := DefaultClient.POST("http://httpbin.org/post").
		SetBodyAsJson(H{ // 设置JSON数据（使用map）
			"name": "wf", // 姓名字段
			"age":  18,   // 年龄字段
		}).Do() // 执行请求

	// 方式2: 支持结构体对象类型（注释掉的代码）
	// person := &Person{
	//     Name: "wf",
	//     Age:  18,
	// }
	// res, err := DefaultClient.POST("http://httpbin.org/post").SetBodyAsJson(person).Do()

	// 方式3: 支持JSON字符串类型（注释掉的代码）
	// jsonText := "{\"Name\":\"wf\",\"Age\":18}"
	// res, err := DefaultClient.POST("http://httpbin.org/post").SetBodyAsJson(jsonText).Do()

	if err != nil {
		t.Error("post failed", err) // 记录POST请求失败错误
	}

	// 确保响应体在函数结束时关闭
	defer res.Body.Close()

	// 验证HTTP状态码
	if res.StatusCode != 200 {
		t.Error("Status Code not 200")
	}

	// 读取响应体内容
	data, err := io.ReadAll(res.Body)
	if nil != err {
		t.Error("read failed", err) // 记录读取响应失败错误
	}

	// 打印响应内容，可以看到服务器接收到的JSON数据
	fmt.Println(string(data))
}

// TestHttpClientPostMultipart 测试POST多部分表单数据提交功能
// 使用multipart/form-data格式提交包含文件的表单数据
func TestHttpClientPostMultipart(t *testing.T) {
	// Content-Type 会自动设置为: multipart/form-data
	// 当表单数据中包含FormFile类型时，会自动使用multipart格式
	res, err := DefaultClient.POST("http://httpbin.org/post").
		SetBodyAsForm(H{ // 设置多部分表单数据
			"name":   "wf",                       // 普通文本字段
			"age":    18,                         // 数字字段（会转换为字符串）
			"photo":  FormFile("D:\\4B.jpg"),     // 图片文件字段
			"resume": FormFile("D:\\resume.rtf"), // 简历文件字段
		}).Do() // 执行请求

	if err != nil {
		t.Error("post failed", err) // 记录POST请求失败错误
	}

	// 确保响应体在函数结束时关闭
	defer res.Body.Close()

	// 验证HTTP状态码
	if res.StatusCode != 200 {
		t.Error("Status Code not 200")
	}

	// 读取响应体内容
	data, err := io.ReadAll(res.Body)
	if nil != err {
		t.Error("read failed", err) // 记录读取响应失败错误
	}

	// 打印响应内容，可以看到服务器接收到的多部分表单数据
	fmt.Println(string(data))
}

// TestHttpClientDelete 测试HTTP DELETE请求功能
// 发送DELETE请求并获取字符串响应
func TestHttpClientDelete(t *testing.T) {
	// 发送DELETE请求到测试服务器并直接获取字符串响应
	str, err := DefaultClient.DELETE("http://httpbin.org/delete").DoForString()
	if err != nil {
		t.Error("delete failed", err) // 记录DELETE请求失败错误
	}

	// 打印响应内容
	println(str)
}

// TestHttpClientPutForm 测试PUT表单提交功能
// 使用application/x-www-form-urlencoded格式提交表单数据
func TestHttpClientPutForm(t *testing.T) {
	// Content-Type 会自动设置为: application/x-www-form-urlencoded
	// 发送PUT请求，提交表单数据
	res, err := DefaultClient.PUT("http://httpbin.org/put").
		SetBodyAsForm(H{ // 设置表单数据
			"name": "wf", // 姓名字段
			"age":  18,   // 年龄字段
		}).Do() // 执行请求

	if err != nil {
		t.Error("put failed", err) // 记录PUT请求失败错误
	}

	// 确保响应体在函数结束时关闭
	defer res.Body.Close()

	// 验证HTTP状态码
	if res.StatusCode != 200 {
		t.Error("Status Code not 200")
	}

	// 读取响应体内容
	data, err := io.ReadAll(res.Body)
	if nil != err {
		t.Error("read failed", err) // 记录读取响应失败错误
	}

	// 打印响应内容
	fmt.Println(string(data))
}

// TestHttpClientPutJson 测试PUT JSON数据提交功能
// 使用application/json格式提交JSON数据
func TestHttpClientPutJson(t *testing.T) {
	// Content-Type 会自动设置为: application/json
	// 发送PUT请求，提交JSON数据
	res, err := DefaultClient.PUT("http://httpbin.org/put").
		SetBodyAsJson(H{ // 设置JSON数据
			"name": "wf", // 姓名字段
			"age":  18,   // 年龄字段
		}).Do() // 执行请求

	// 同样支持其他数据类型（注释掉的代码）:
	// 支持结构体对象类型
	// person := &Person{
	//     Name: "wf",
	//     Age:  18,
	// }
	// res, err := DefaultClient.PUT("http://httpbin.org/put").SetBodyAsJson(person).Do()

	// 支持JSON字符串类型
	// jsonText := "{\"Name\":\"wf\",\"Age\":18}"
	// res, err := DefaultClient.PUT("http://httpbin.org/put").SetBodyAsJson(jsonText).Do()

	if err != nil {
		t.Error("put failed", err) // 记录PUT请求失败错误（注意：这里应该是"put failed"）
	}

	// 确保响应体在函数结束时关闭
	defer res.Body.Close()

	// 验证HTTP状态码
	if res.StatusCode != 200 {
		t.Error("Status Code not 200")
	}

	// 读取响应体内容
	data, err := io.ReadAll(res.Body)
	if nil != err {
		t.Error("read failed", err) // 记录读取响应失败错误
	}

	// 打印响应内容
	fmt.Println(string(data))
}

// TestResources 测试带认证头的资源获取功能
// 演示如何添加Authorization头并获取响应实体
func TestResources(t *testing.T) {
	// 发送带有Authorization头的GET请求，获取用户资源信息
	entity, err := DefaultClient.
		AddHeader("Authorization", "adc8620e5164462e854f6f2e4e33ee53"). // 添加认证头
		GET("http://localhost:8090/portal/users/admin/resources").      // 请求本地服务的资源接口
		DoForEntity()                                                   // 执行请求并获取响应实体（包含状态码和数据）

	if err != nil {
		fmt.Println(err) // 打印错误信息
	}

	// 打印响应内容，DoForEntity返回的实体可以方便地获取响应数据
	fmt.Println("RespondText:", entity.DataAsString())
}
