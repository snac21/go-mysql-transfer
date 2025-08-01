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

// Package luaengine Redis操作执行器
// 为Lua脚本提供Redis操作功能，支持Redis的5种基本数据结构
// 包括String、Hash、List、Set、Sorted Set的增删操作
package luaengine

import (
	"github.com/go-mysql-org/go-mysql/canal" // Canal binlog解析
	lua "github.com/yuin/gopher-lua"         // Lua虚拟机

	"go-mysql-transfer/global"          // 全局配置
	"go-mysql-transfer/model"           // 数据模型
	"go-mysql-transfer/util/stringutil" // 字符串工具
)

// _globalOLDROW UPDATE操作时的旧行数据全局变量名
// 在UPDATE操作中，Lua脚本可以通过此变量访问修改前的数据
const _globalOLDROW = "___OLDROW___"

// redisModule 创建Redis操作模块
// 在Lua脚本中通过require("redisOps")加载此模块
// L: Lua状态实例
// 返回值数量：1（Redis操作模块表）
func redisModule(L *lua.LState) int {
	t := L.NewTable()              // 创建新的Lua表
	L.SetFuncs(t, _redisModuleApi) // 设置模块API函数
	L.Push(t)                      // 将表推入Lua栈
	return 1                       // 返回1个值
}

// _redisModuleApi Redis模块API函数映射表
// 定义了Lua脚本中可以调用的所有Redis操作函数
var _redisModuleApi = map[string]lua.LGFunction{
	// 基础函数
	"rawRow":    rawRow,    // 获取当前行数据
	"rawOldRow": rawOldRow, // 获取旧行数据（UPDATE操作）
	"rawAction": rawAction, // 获取操作类型

	// String类型操作
	"SET": redisSet, // 设置字符串值
	"DEL": redisDel, // 删除键

	// Hash类型操作
	"HSET": redisHSet, // 设置哈希字段
	"HDEL": redisHDel, // 删除哈希字段

	// List类型操作
	"RPUSH": redisRPush, // 右侧推入列表
	"LREM":  redisLRem,  // 从列表移除元素

	// Set类型操作
	"SADD": redisSAdd, // 添加集合成员
	"SREM": redisSRem, // 移除集合成员

	// Sorted Set类型操作
	"ZADD": redisZAdd, // 添加有序集合成员
	"ZREM": redisZRem, // 移除有序集合成员
}

// rawOldRow Lua函数：获取UPDATE操作的旧行数据
// 在UPDATE操作的Lua脚本中调用，返回修改前的数据行
// 返回值数量：1（旧行数据表）
func rawOldRow(L *lua.LState) int {
	row := L.GetGlobal(_globalOLDROW) // 获取全局变量中的旧行数据
	L.Push(row)                       // 将旧行数据推入Lua栈
	return 1                          // 返回1个值
}

// redisSet Lua函数：Redis SET操作
// 在Lua脚本中调用：SET(key, value)
// 对应Redis的String数据结构，设置键值对
// 参数1：key - Redis键名
// 参数2：val - 要设置的值
// 返回值数量：0
func redisSet(L *lua.LState) int {
	key := L.CheckString(1) // 获取第1个参数：键名
	val := L.CheckAny(2)    // 获取第2个参数：值（任意类型）

	// 获取全局返回结果表
	ret := L.GetGlobal(_globalRET)

	// 将操作记录到返回结果中
	// 格式："insert_1_" + key，其中1表示String类型
	L.SetTable(ret, lua.LString("insert_1_"+key), val)
	return 0 // 不返回值给Lua脚本
}

// redisDel Lua函数：Redis DEL操作
// 在Lua脚本中调用：DEL(key)
// 删除指定的Redis键
// 参数1：key - 要删除的Redis键名
// 返回值数量：0
func redisDel(L *lua.LState) int {
	key := L.CheckString(1) // 获取第1个参数：键名

	// 获取全局返回结果表
	ret := L.GetGlobal(_globalRET)

	// 将删除操作记录到返回结果中
	// 格式："delete_1_" + key，其中1表示String类型
	L.SetTable(ret, lua.LString("delete_1_"+key), lua.LBool(true))
	return 0 // 不返回值给Lua脚本
}

func redisHSet(L *lua.LState) int {
	key := L.CheckString(1)
	field := L.CheckAny(2)
	val := L.CheckAny(3)

	hash := L.NewTable()
	L.SetTable(hash, lua.LString("key"), lua.LString(key))
	L.SetTable(hash, lua.LString("field"), field)
	L.SetTable(hash, lua.LString("val"), val)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString("insert_2_"+stringutil.UUID()), hash)
	return 0
}

func redisHDel(L *lua.LState) int {
	key := L.CheckAny(1)
	field := L.CheckAny(2)

	hash := L.NewTable()
	L.SetTable(hash, lua.LString("key"), key)
	L.SetTable(hash, lua.LString("field"), field)
	L.SetTable(hash, lua.LString("val"), lua.LNumber(1))

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString("delete_2_"+stringutil.UUID()), hash)
	return 0
}

func redisRPush(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckAny(2)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString("insert_3_"+key), val)
	return 0
}

func redisLRem(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckAny(2)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString("delete_3_"+key), val)
	return 0
}

func redisSAdd(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckAny(2)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString("insert_4_"+key), val)
	return 0
}

func redisSRem(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckAny(2)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString("delete_4_"+key), val)
	return 0
}

func redisZAdd(L *lua.LState) int {
	key := L.CheckString(1)
	score := L.CheckAny(2)
	val := L.CheckAny(3)

	hash := L.NewTable()
	L.SetTable(hash, lua.LString("key"), lua.LString(key))
	L.SetTable(hash, lua.LString("score"), score)
	L.SetTable(hash, lua.LString("val"), val)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString("insert_5_"+stringutil.UUID()), hash)
	return 0
}

func redisZRem(L *lua.LState) int {
	key := L.CheckString(1)
	val := L.CheckAny(2)

	ret := L.GetGlobal(_globalRET)
	L.SetTable(ret, lua.LString("delete_5_"+key), val)
	return 0
}

// DoRedisOps 执行Redis操作的Lua脚本
// 这是Redis操作执行器的核心函数，负责执行用户定义的Lua脚本
// input: 当前行数据（新数据）
// previous: 旧行数据（仅UPDATE操作时有效）
// action: 操作类型（INSERT/UPDATE/DELETE）
// rule: 同步规则，包含编译后的Lua脚本
// 返回Redis操作响应列表和可能的错误
func DoRedisOps(input map[string]interface{}, previous map[string]interface{}, action string, rule *global.Rule) ([]*model.RedisRespond, error) {
	// 1. 从池中获取Lua状态实例
	L := _pool.Get()
	defer _pool.Put(L) // 确保使用完后放回池中

	// 2. 准备Lua脚本执行环境
	// 创建当前行数据表并填充数据
	row := L.NewTable()
	paddingTable(L, row, input)

	// 创建返回结果表
	ret := L.NewTable()

	// 设置全局变量，供Lua脚本访问
	L.SetGlobal(_globalRET, ret)                 // 返回结果表
	L.SetGlobal(_globalROW, row)                 // 当前行数据
	L.SetGlobal(_globalACT, lua.LString(action)) // 操作类型

	// 3. 处理UPDATE操作的旧数据
	if action == canal.UpdateAction {
		// 创建旧行数据表并填充数据
		oldRow := L.NewTable()
		paddingTable(L, oldRow, previous)
		L.SetGlobal(_globalOLDROW, oldRow) // 设置旧行数据全局变量
	}

	// 4. 执行Lua脚本
	// 从预编译的原型创建函数
	funcFromProto := L.NewFunctionFromProto(rule.LuaProto)
	L.Push(funcFromProto) // 将函数推入栈

	// 调用Lua函数，0个参数，多个返回值
	err := L.PCall(0, lua.MultRet, nil)
	if err != nil {
		return nil, err // 脚本执行失败
	}

	// 5. 解析脚本执行结果
	// 创建响应列表，预分配容量
	ls := make([]*model.RedisRespond, 0, ret.Len())

	// 遍历返回结果表中的所有操作
	ret.ForEach(func(k lua.LValue, v lua.LValue) {
		resp := new(model.RedisRespond)
		kk := lvToString(k) // 将键转换为字符串

		// 解析操作信息
		// 键格式："{action}_{structure}_{key}"
		// 例如："insert_1_mykey" 表示对String类型的mykey执行插入操作
		resp.Action = kk[0:6]                   // 前6个字符是操作类型
		resp.Structure = structureName(kk[7:8]) // 第8个字符是数据结构类型

		// 根据操作类型处理不同的数据格式
		if resp.Action == canal.DeleteAction {
			// 删除操作：键名从第9个字符开始
			resp.Key = kk[9:len(kk)]
			resp.Val = lvToInterface(v, true)
		} else {
			// 插入/更新操作：根据数据结构类型处理
			if resp.Structure == global.RedisStructureHash {
				// Hash类型：需要解析key、field、val
				key := L.GetTable(v, lua.LString("key"))
				field := L.GetTable(v, lua.LString("field"))
				val := L.GetTable(v, lua.LString("val"))
				resp.Key = key.String()
				resp.Field = lvToString(field)
				resp.Val = lvToInterface(val, true)
			} else if resp.Structure == global.RedisStructureSortedSet {
				// Sorted Set类型：需要解析key、score、val
				key := L.GetTable(v, lua.LString("key"))
				score := L.GetTable(v, lua.LString("score"))
				val := L.GetTable(v, lua.LString("val"))
				resp.Key = key.String()
				scoreTemp := lvToString(score)
				resp.Score = stringutil.ToFloat64Safe(scoreTemp) // 转换分数为浮点数
				resp.Val = lvToInterface(val, true)
			} else {
				// String、List、Set类型：直接使用键和值
				resp.Key = kk[9:len(kk)]
				resp.Val = lvToInterface(v, true)
			}
		}

		// 将响应添加到列表中
		ls = append(ls, resp)
	})

	return ls, nil // 返回所有Redis操作响应
}

func structureName(code string) string {
	switch code {
	case "1":
		return global.RedisStructureString
	case "2":
		return global.RedisStructureHash
	case "3":
		return global.RedisStructureList
	case "4":
		return global.RedisStructureSet
	case "5":
		return global.RedisStructureSortedSet
	}

	return ""
}
