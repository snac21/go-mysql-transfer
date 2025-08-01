// Package httpclient 的HTTP响应处理模块
// 提供了HTTP响应的封装和便捷处理方法
package httpclient

import (
	"encoding/json" // JSON编码解码
	"net/http"      // HTTP状态码处理
)

// RespondEntity HTTP响应实体结构体
// 封装了HTTP响应的状态码和数据，提供便捷的数据访问方法
// 相比直接使用http.Response，提供了更友好的API和自动资源管理
type RespondEntity struct {
	statusCode int    // HTTP状态码（如200、404、500等）
	data       []byte // 响应体数据的字节数组
}

// StatusCode 获取HTTP状态码
// 返回响应的HTTP状态码，如200表示成功，404表示未找到等
func (t *RespondEntity) StatusCode() int {
	return t.statusCode
}

// StatusText 获取HTTP状态码对应的文本描述
// 返回状态码的标准文本描述，如200对应"OK"，404对应"Not Found"
// 使用Go标准库的http.StatusText函数进行转换
func (t *RespondEntity) StatusText() string {
	return http.StatusText(t.statusCode)
}

// Data 获取响应体的原始字节数据
// 返回响应体的完整字节数组，适用于需要处理二进制数据的场景
// 如图片、文件下载等
func (t *RespondEntity) Data() []byte {
	return t.data
}

// DataAsString 获取响应体的字符串形式
// 将响应体字节数组转换为字符串返回
// 适用于文本类型的响应，如HTML、JSON、XML等
func (t *RespondEntity) DataAsString() string {
	return string(t.data)
}

// Unmarshal 将响应体JSON数据反序列化到指定对象
// entity: 目标对象指针，用于接收反序列化后的数据
// 返回反序列化过程中可能出现的错误
// 适用于API返回JSON格式数据的场景
// 使用示例：
//
//	var user User
//	err := response.Unmarshal(&user)
func (t *RespondEntity) Unmarshal(entity interface{}) error {
	err := json.Unmarshal(t.data, entity) // 使用标准库进行JSON反序列化
	if err != nil {
		return err // 反序列化失败时返回错误
	}
	return nil // 成功时返回nil
}
