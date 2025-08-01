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

// Package luaengine Lua脚本引擎模块
// 提供Lua脚本执行环境，支持数据库操作、HTTP请求、Redis操作等功能
// 通过Lua脚本实现复杂的数据处理逻辑，提供高度的灵活性和可扩展性
package luaengine

import (
	"encoding/json" // JSON编码解码
	"sync"          // 同步原语

	"github.com/go-mysql-org/go-mysql/canal" // Canal binlog解析
	luaJson "github.com/layeh/gopher-json"   // Lua JSON支持库
	lua "github.com/yuin/gopher-lua"         // Lua虚拟机

	"go-mysql-transfer/util/byteutil"   // 字节工具
	"go-mysql-transfer/util/httpclient" // HTTP客户端
	"go-mysql-transfer/util/stringutil" // 字符串工具
)

// Lua脚本全局变量名常量
// 这些变量在Lua脚本执行时会被设置，供脚本访问
const (
	_globalRET = "___RET___" // 返回结果变量名，存储脚本执行结果
	_globalROW = "___ROW___" // 当前行数据变量名，存储当前处理的数据行
	_globalACT = "___ACT___" // 操作类型变量名，存储当前操作类型（INSERT/UPDATE/DELETE）
)

// 全局变量
var (
	_pool       *luaStatePool          // Lua状态池，复用Lua虚拟机实例
	_ds         *canal.Canal           // Canal实例，用于数据库操作
	_httpClient *httpclient.HttpClient // HTTP客户端，用于HTTP请求
)

// luaStatePool Lua状态池结构体
// 管理Lua虚拟机实例的创建、获取和回收，提高性能
// 避免频繁创建和销毁Lua虚拟机的开销
type luaStatePool struct {
	lock  sync.Mutex    // 互斥锁，保证线程安全
	saved []*lua.LState // 保存的Lua状态实例切片
}

// InitActuator 初始化Lua脚本执行器
// 设置Canal实例并创建Lua状态池
// ds: Canal实例，用于数据库操作
func InitActuator(ds *canal.Canal) {
	_ds = ds // 保存Canal实例到全局变量

	// 创建Lua状态池，初始容量为3
	_pool = &luaStatePool{
		saved: make([]*lua.LState, 0, 3),
	}
}

// Get 从池中获取一个Lua状态实例
// 如果池中没有可用实例，则创建新的实例
// 返回可用的Lua状态实例
func (p *luaStatePool) Get() *lua.LState {
	p.lock.Lock()         // 加锁保证线程安全
	defer p.lock.Unlock() // 函数结束时解锁

	n := len(p.saved) // 获取池中实例数量
	if n == 0 {
		// 池中没有可用实例，创建新实例
		return p.New()
	}

	// 从池中取出最后一个实例（栈顶）
	x := p.saved[n-1]
	p.saved = p.saved[0 : n-1] // 更新切片，移除已取出的实例
	return x
}

// New 创建新的Lua状态实例
// 初始化Lua虚拟机并预加载所有必要的模块
// 返回配置完成的Lua状态实例
func (p *luaStatePool) New() *lua.LState {
	// 创建HTTP客户端实例
	_httpClient = httpclient.NewClient()

	// 创建新的Lua虚拟机状态
	L := lua.NewState()

	// 预加载JSON支持库，提供JSON编码解码功能
	luaJson.Preload(L)

	// 预加载各种操作模块，供Lua脚本调用
	L.PreloadModule("scriptOps", scriptModule) // 脚本操作模块
	L.PreloadModule("dbOps", dbModule)         // 数据库操作模块
	L.PreloadModule("httpOps", httpModule)     // HTTP操作模块

	L.PreloadModule("redisOps", redisModule)   // Redis操作模块
	L.PreloadModule("mqOps", mqModule)         // 消息队列操作模块
	L.PreloadModule("mongodbOps", mongoModule) // MongoDB操作模块
	L.PreloadModule("esOps", esModule)         // Elasticsearch操作模块

	return L // 返回配置完成的Lua状态
}

// Put 将Lua状态实例放回池中
// 实现实例的复用，避免频繁创建和销毁的开销
// L: 要放回池中的Lua状态实例
func (p *luaStatePool) Put(L *lua.LState) {
	p.lock.Lock()         // 加锁保证线程安全
	defer p.lock.Unlock() // 函数结束时解锁

	// 将实例添加到池中
	p.saved = append(p.saved, L)
}

// Shutdown 关闭状态池
// 关闭池中所有的Lua状态实例，释放资源
// 通常在应用程序关闭时调用
func (p *luaStatePool) Shutdown() {
	// 遍历池中所有实例并关闭
	for _, L := range p.saved {
		L.Close() // 关闭Lua虚拟机，释放内存
	}
}

// rawRow Lua函数：获取原始行数据
// 在Lua脚本中调用，返回当前处理的数据行
// 返回值数量：1（当前行数据表）
func rawRow(L *lua.LState) int {
	row := L.GetGlobal(_globalROW) // 获取全局变量中的行数据
	L.Push(row)                    // 将行数据推入Lua栈
	return 1                       // 返回1个值
}

// rawAction Lua函数：获取当前操作类型
// 在Lua脚本中调用，返回当前的操作类型（INSERT/UPDATE/DELETE）
// 返回值数量：1（操作类型字符串）
func rawAction(L *lua.LState) int {
	act := L.GetGlobal(_globalACT) // 获取全局变量中的操作类型
	L.Push(act)                    // 将操作类型推入Lua栈
	return 1                       // 返回1个值
}

// paddingTable 将Go的map数据填充到Lua表中
// 处理各种Go数据类型到Lua类型的转换
// l: Lua状态实例
// table: 目标Lua表
// kv: 要填充的键值对数据
func paddingTable(l *lua.LState, table *lua.LTable, kv map[string]interface{}) {
	// 遍历所有键值对
	for k, v := range kv {
		// 根据值的类型进行相应的转换
		switch v.(type) {
		// 浮点数类型转换
		case float64:
			ft := v.(float64)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))
		case float32:
			ft := v.(float32)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))

		// 整数类型转换（有符号）
		case int:
			ft := v.(int)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))
		case int8:
			ft := v.(int8)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))
		case int16:
			ft := v.(int16)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))
		case int32:
			ft := v.(int32)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))
		case int64:
			ft := v.(int64)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))

		// 整数类型转换（无符号）
		case uint:
			ft := v.(uint)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))
		case uint8:
			ft := v.(uint8)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))
		case uint16:
			ft := v.(uint16)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))
		case uint32:
			ft := v.(uint32)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))
		case uint64:
			ft := v.(uint64)
			l.SetTable(table, lua.LString(k), lua.LNumber(ft))

		// 字符串类型转换
		case string:
			ft := v.(string)
			l.SetTable(table, lua.LString(k), lua.LString(ft))

		// 字节数组转换为字符串
		case []byte:
			ft := string(v.([]byte))
			l.SetTable(table, lua.LString(k), lua.LString(ft))

		// nil值处理
		case nil:
			l.SetTable(table, lua.LString(k), lua.LNil)

		// 其他复杂类型转换为JSON字符串
		default:
			jsonValue, _ := json.Marshal(v)
			l.SetTable(table, lua.LString(k), lua.LString(jsonValue))
		}
	}
}

func lvToString(lv lua.LValue) string {
	if lua.LVCanConvToString(lv) {
		return lua.LVAsString(lv)
	}

	return lv.String()
}

func lvToByteArray(lv lua.LValue) []byte {
	switch lv.Type() {
	case lua.LTNil:
		return nil
	case lua.LTBool:
		return byteutil.JsonBytes(lua.LVAsBool(lv))
	case lua.LTNumber:
		return []byte(lv.String())
	case lua.LTString:
		return []byte(lua.LVAsString(lv))
	case lua.LTTable:
		ret := lvToInterface(lv, false)
		return byteutil.JsonBytes(ret)
	default:
		return byteutil.JsonBytes(lv)
	}
}

func lvToInterface(lv lua.LValue, tableToJson bool) interface{} {
	switch lv.Type() {
	case lua.LTNil:
		return nil
	case lua.LTBool:
		return lua.LVAsBool(lv)
	case lua.LTNumber:
		return float64(lua.LVAsNumber(lv))
	case lua.LTString:
		return lua.LVAsString(lv)
	case lua.LTTable:
		t, _ := lv.(*lua.LTable)
		len := t.MaxN()
		if len == 0 { // table
			ret := make(map[string]interface{})
			t.ForEach(func(key, value lua.LValue) {
				ret[lvToString(key)] = lvToInterface(value, false)
			})
			if tableToJson {
				return stringutil.ToJsonString(ret)
			}
			return ret
		} else { // array
			ret := make([]interface{}, 0, len)
			for i := 1; i <= len; i++ {
				ret = append(ret, lvToInterface(t.RawGetInt(i), false))
			}
			if tableToJson {
				return stringutil.ToJsonString(ret)
			}
			return ret
		}
	default:
		return lv
	}
}

func lvToMap(lv lua.LValue) (map[string]interface{}, bool) {
	switch lv.Type() {
	case lua.LTTable:
		t := lvToInterface(lv, false)
		ret := t.(map[string]interface{})
		return ret, true
	default:
		return nil, false
	}
}

func interfaceToLv(v interface{}) lua.LValue {
	switch v.(type) {
	case float64:
		ft := v.(float64)
		return lua.LNumber(ft)
	case float32:
		ft := v.(float32)
		return lua.LNumber(ft)
	case int:
		ft := v.(int)
		return lua.LNumber(ft)
	case uint:
		ft := v.(uint)
		return lua.LNumber(ft)
	case int8:
		ft := v.(int8)
		return lua.LNumber(ft)
	case uint8:
		ft := v.(uint8)
		return lua.LNumber(ft)
	case int16:
		ft := v.(int16)
		return lua.LNumber(ft)
	case uint16:
		ft := v.(uint16)
		return lua.LNumber(ft)
	case int32:
		ft := v.(int32)
		return lua.LNumber(ft)
	case uint32:
		ft := v.(uint32)
		return lua.LNumber(ft)
	case int64:
		ft := v.(int64)
		return lua.LNumber(ft)
	case uint64:
		ft := v.(uint64)
		return lua.LNumber(ft)
	case string:
		ft := v.(string)
		return lua.LString(ft)
	case []byte:
		ft := string(v.([]byte))
		return lua.LString(ft)
	case nil:
		return lua.LNil
	default:
		jsonValue, _ := json.Marshal(v)
		return lua.LString(jsonValue)
	}

}
