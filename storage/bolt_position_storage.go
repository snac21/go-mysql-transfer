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

// Package storage BoltDB位置存储实现模块
// 使用BoltDB嵌入式数据库存储MySQL binlog位置信息
// BoltDB是一个纯Go实现的键值数据库，适合单机部署场景
package storage

import (
	"github.com/go-mysql-org/go-mysql/mysql" // MySQL相关类型
	"github.com/juju/errors"                 // 错误处理增强
	"github.com/vmihailenco/msgpack"         // MessagePack序列化
	"go.etcd.io/bbolt"                       // BoltDB数据库
)

// boltPositionStorage BoltDB位置存储实现
// 实现了PositionStorage接口，使用BoltDB作为存储后端
// 适用于单机部署模式，数据存储在本地文件中
type boltPositionStorage struct {
	Name string // binlog文件名（结构体字段，但实际使用mysql.Position）
	Pos  uint32 // binlog位置偏移量（结构体字段，但实际使用mysql.Position）
}

// Initialize 初始化BoltDB位置存储
// 检查是否已存在位置记录，如果不存在则创建一个空的位置记录
// 这确保了后续的读取操作不会因为记录不存在而失败
// 返回初始化过程中的错误
func (s *boltPositionStorage) Initialize() error {
	// 使用写事务更新数据库
	return _bolt.Update(func(tx *bbolt.Tx) error {
		// 获取位置信息存储桶
		bt := tx.Bucket(_positionBucket)

		// 检查是否已存在位置记录
		data := bt.Get(_fixPositionId)
		if data != nil {
			// 记录已存在，无需初始化
			return nil
		}

		// 创建空的位置记录并序列化
		bytes, err := msgpack.Marshal(mysql.Position{})
		if err != nil {
			return err // 序列化失败
		}

		// 保存初始位置记录到数据库
		return bt.Put(_fixPositionId, bytes)
	})
}

// Save 保存binlog位置信息到BoltDB
// pos: 要保存的MySQL binlog位置信息
// 使用MessagePack进行序列化，提供高效的二进制存储
// 返回保存过程中的错误
func (s *boltPositionStorage) Save(pos mysql.Position) error {
	// 使用写事务更新数据库
	return _bolt.Update(func(tx *bbolt.Tx) error {
		// 获取位置信息存储桶
		bt := tx.Bucket(_positionBucket)

		// 将位置信息序列化为字节数组
		data, err := msgpack.Marshal(pos)
		if err != nil {
			return err // 序列化失败
		}

		// 保存序列化后的数据到数据库
		// 使用固定的键_fixPositionId，确保只有一条位置记录
		return bt.Put(_fixPositionId, data)
	})
}

// Get 从BoltDB获取当前保存的binlog位置信息
// 返回位置信息和可能的错误
// 如果位置记录不存在，返回NotFound错误
func (s *boltPositionStorage) Get() (mysql.Position, error) {
	var entity mysql.Position // 用于存储反序列化后的位置信息

	// 使用只读事务查询数据库
	err := _bolt.View(func(tx *bbolt.Tx) error {
		// 获取位置信息存储桶
		bt := tx.Bucket(_positionBucket)

		// 根据固定键获取位置数据
		data := bt.Get(_fixPositionId)
		if data == nil {
			// 位置记录不存在
			return errors.NotFoundf("PositionStorage")
		}

		// 反序列化位置数据
		return msgpack.Unmarshal(data, &entity)
	})

	return entity, err
}
