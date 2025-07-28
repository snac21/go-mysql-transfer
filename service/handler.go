package service

import (
	"go-mysql-transfer/metrics"
	"log"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/juju/errors"

	"go-mysql-transfer/global"
	"go-mysql-transfer/model"
	"go-mysql-transfer/util/logs"
)

// handler MySQL binlog 事件处理器
// 负责接收和处理 MySQL binlog 中的各种事件，如数据变更、DDL操作、事务提交等
type handler struct {
	queue chan interface{} // 事件队列，用于缓存待处理的事件
	stop  chan struct{}    // 停止信号通道，用于优雅关闭处理器
}

// newHandler 创建新的事件处理器实例
// 返回初始化完成的 handler 实例，包含事件队列和停止信号通道
func newHandler() *handler {
	return &handler{
		queue: make(chan interface{}, 4096), // 创建容量为4096的事件队列，避免阻塞
		stop:  make(chan struct{}, 1),       // 创建容量为1的停止信号通道
	}
}

// OnRotate 处理 binlog 文件轮转事件
// 当 MySQL 切换到新的 binlog 文件时触发，需要更新位置信息以确保连续性
// header: 事件头信息，包含时间戳、位置等元数据
// e: 轮转事件，包含新的 binlog 文件名和起始位置
func (s *handler) OnRotate(header *replication.EventHeader, e *replication.RotateEvent) error {
	// 记录轮转事件的详细信息，便于调试和监控
	// 注意：header.LogPos 是旧文件中ROTATE事件的位置，e.Position 是新文件的起始位置
	logs.Infof("Binlog rotate event: ROTATE at old file position %d, switch to file %s at position %d",
		header.LogPos, string(e.NextLogName), e.Position)

	// 关键决策：必须使用 e.Position 而不是 header.LogPos
	// 原因：header.LogPos = 旧文件中ROTATE事件的位置
	//      e.Position = 新文件的起始位置（通常是4，binlog文件头大小）
	// 我们需要保存新文件的起始位置以便后续从新文件读取
	s.queue <- model.PosRequest{
		Name:  string(e.NextLogName), // 新的 binlog 文件名
		Pos:   uint32(e.Position),    // 必须用新文件起始位置，不能用header.LogPos
		Force: true,                  // 强制保存，确保位置信息及时更新
	}
	return nil
}

// OnTableChanged 处理表结构变更事件
// 当表被创建、修改、重命名或删除时触发，需要更新相关的规则和缓存
// header: 事件头信息
// schema: 数据库名
// table: 表名
func (s *handler) OnTableChanged(header *replication.EventHeader, schema, table string) error {
	// 记录表结构变更事件
	logs.Infof("Table changed event: %s.%s at position %d, timestamp: %d",
		schema, table, header.LogPos, header.Timestamp)

	// 更新传输规则，清除相关缓存
	err := _transferService.updateRule(schema, table)
	if err != nil {
		// 记录错误并返回，保持错误堆栈信息
		logs.Errorf("Failed to update rule for table %s.%s: %v", schema, table, err)
		return errors.Trace(err)
	}

	logs.Infof("Successfully updated rule for table %s.%s", schema, table)
	return nil
}

// OnDDL 处理 DDL（数据定义语言）事件
// 当执行 CREATE、ALTER、DROP 等 DDL 语句时触发
// header: 事件头信息
// nextPos: 下一个事件的位置
// queryEvent: 查询事件，包含执行的 SQL 语句
func (s *handler) OnDDL(header *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	// 记录 DDL 事件的详细信息
	if queryEvent != nil {
		logs.Infof("DDL event: %s at nextPos %s:%d, header.LogPos: %d",
			string(queryEvent.Query), nextPos.Name, nextPos.Pos, header.LogPos)
	}

	// 位置选择策略分析：
	// nextPos.Pos - 下一个事件的起始位置（DDL事件之后的位置）
	// header.LogPos - 当前DDL事件的结束位置
	// 理论上：nextPos.Pos 应该等于 header.LogPos（或者非常接近）

	pos := nextPos // 优先使用 nextPos，因为它指向下一个事件的位置

	// 如果 nextPos 信息不完整，使用 header.LogPos 作为备选
	if pos.Name == "" || pos.Pos == 0 {
		logs.Warnf("nextPos is incomplete (%s:%d), fallback to header.LogPos %d",
			pos.Name, pos.Pos, header.LogPos)
		if header.LogPos > 0 {
			pos.Pos = header.LogPos
		}
	} else if header.LogPos > 0 && pos.Pos != header.LogPos {
		// 🔍 调试信息：记录位置差异，帮助分析问题
		logs.Debugf("Position difference detected: nextPos=%d, header.LogPos=%d, diff=%d",
			pos.Pos, header.LogPos, int64(pos.Pos)-int64(header.LogPos))
	}

	// 将位置更新请求放入队列，DDL 事件需要强制保存位置
	s.queue <- model.PosRequest{
		Name:  pos.Name, // binlog 文件名
		Pos:   pos.Pos,  // 使用 nextPos，因为我们需要从下一个事件开始读取
		Force: true,     // DDL 事件强制保存，确保一致性
	}
	return nil
}

// OnXID 处理事务提交事件
// 当事务通过 XID 事件提交时触发，这是 InnoDB 存储引擎的事务提交标志
// header: 事件头信息
// nextPos: 下一个事件的位置
func (s *handler) OnXID(header *replication.EventHeader, nextPos mysql.Position) error {
	// 记录事务提交事件
	logs.Debugf("XID event: nextPos %s:%d, header.LogPos: %d",
		nextPos.Name, nextPos.Pos, header.LogPos)

	// 位置选择策略分析：
	// nextPos.Pos - 下一个事件的起始位置（XID事件之后的位置）
	// header.LogPos - 当前XID事件的结束位置
	// 对于XID事件：nextPos.Pos 通常等于 header.LogPos

	pos := nextPos

	// 对于XID事件，我们应该优先使用哪个位置？
	// 选择策略：优先使用 nextPos，但如果 header.LogPos 更大，则使用 header.LogPos
	// 原因：确保位置的单调递增，避免位置回退
	if header.LogPos > 0 {
		if header.LogPos > pos.Pos {
			// header.LogPos 更大，使用它确保位置不回退
			logs.Debugf("Using header.LogPos %d (larger than nextPos %d)", header.LogPos, pos.Pos)
			pos.Pos = header.LogPos
		} else if header.LogPos < pos.Pos {
			// nextPos 更大，记录差异但仍使用 nextPos
			logs.Debugf("Using nextPos %d (larger than header.LogPos %d)", pos.Pos, header.LogPos)
		} else {
			// 两者相等，这是正常情况
			logs.Debugf("nextPos equals header.LogPos: %d", pos.Pos)
		}
	}

	// 将位置更新请求放入队列，Force=false 表示可以批量保存
	s.queue <- model.PosRequest{
		Name:  pos.Name, // binlog 文件名
		Pos:   pos.Pos,  // 使用经过比较后的最大位置值
		Force: false,    // 非强制保存，可以批量处理以提高性能
	}
	return nil
}

// OnGTID 处理 GTID（全局事务标识符）事件
// 在启用 GTID 模式的 MySQL 主从复制中，每个事务都有唯一的 GTID
// header: 事件头信息
// gtidEvent: GTID 事件，包含事务的全局唯一标识符
func (s *handler) OnGTID(header *replication.EventHeader, gtidEvent mysql.BinlogGTIDEvent) error {
	// 记录 GTID 事件信息，用于调试和监控
	if header != nil {
		logs.Debugf("GTID event at position %d, timestamp: %d, server_id: %d",
			header.LogPos, header.Timestamp, header.ServerID)
	}

	// 可以在这里实现 GTID 相关的逻辑，如：
	// 1. 记录 GTID 信息用于断点续传
	// 2. 跳过已处理的事务
	// 3. 实现基于 GTID 的位置恢复

	// 当前实现暂时不处理 GTID，直接返回成功
	return nil
}

// OnPosSynced 处理位置同步事件
// 用于实现自定义的位置同步逻辑，当需要同步位置时被调用
// header: 事件头信息
// pos: 当前位置
// set: GTID 集合（如果启用了 GTID）
// force: 是否强制立即同步
func (s *handler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	// 根据 force 参数和 header 信息决定是否记录同步事件
	if force || (header != nil) {
		// 获取时间戳，如果 header 为空则使用 0
		timestamp := uint32(0)
		if header != nil {
			timestamp = header.Timestamp
		}

		// 记录位置同步信息
		logs.Debugf("Position synced: %s:%d, force: %v, timestamp: %d, gtid_set: %v",
			pos.Name, pos.Pos, force, timestamp, set)
	}

	// 这里可以实现自定义的位置同步逻辑，如：
	// 1. 将位置信息同步到外部系统
	// 2. 触发位置检查点
	// 3. 更新监控指标

	return nil
}

// OnRowsQueryEvent 处理行查询事件
// 当设置 binlog_rows_query_log_events=ON 时，每个 DML 查询都会触发此事件
// 可以获取原始执行的 SQL 查询语句，包括注释
// e: 行查询事件，包含原始 SQL 语句
func (s *handler) OnRowsQueryEvent(e *replication.RowsQueryEvent) error {
	// 检查事件是否有效且包含查询语句
	if e != nil && len(e.Query) > 0 {
		// 记录原始 SQL 查询，用于调试和审计
		query := string(e.Query)
		logs.Debugf("Rows query event: %s", query)

		// 可以在这里实现额外的逻辑，如：
		// 1. SQL 审计日志
		// 2. 慢查询分析
		// 3. 查询统计
	}

	return nil
}

// OnRow 处理行变更事件
// 当表中的数据发生 INSERT、UPDATE、DELETE 操作时触发
// 这是数据同步的核心方法，负责处理具体的数据变更
// e: 行事件，包含变更的数据和操作类型
func (s *handler) OnRow(e *canal.RowsEvent) error {
	// 生成规则键，用于查找对应的传输规则
	ruleKey := global.RuleKey(e.Table.Schema, e.Table.Name)

	// 检查是否存在对应的传输规则，如果不存在则跳过处理
	if !global.RuleInsExist(ruleKey) {
		logs.Debugf("No rule found for table %s.%s, skipping", e.Table.Schema, e.Table.Name)
		return nil
	}

	// 记录行变更事件的基本信息
	logs.Debugf("Row event: action=%s, table=%s.%s, rows=%d",
		e.Action, e.Table.Schema, e.Table.Name, len(e.Rows))

	// 声明请求切片，用于存储处理后的行请求
	var requests []*model.RowRequest

	// 对于非 UPDATE 操作，预分配切片容量以提高性能
	if e.Action != canal.UpdateAction {
		requests = make([]*model.RowRequest, 0, len(e.Rows))
	}

	// 处理 UPDATE 操作
	if e.Action == canal.UpdateAction {
		// UPDATE 操作的行数据是成对出现的：奇数索引是旧值，偶数索引是新值
		for i := 0; i < len(e.Rows); i++ {
			// 只处理偶数索引的行（新值）
			if (i+1)%2 == 0 {
				// 创建新的行请求对象
				v := new(model.RowRequest)
				v.RuleKey = ruleKey              // 设置规则键
				v.Action = e.Action              // 设置操作类型
				v.Timestamp = e.Header.Timestamp // 设置时间戳

				// 如果配置了保留原始数据，则保存旧值
				if global.Cfg().IsReserveRawData() {
					v.Old = e.Rows[i-1] // 前一行是旧值
				}

				v.Row = e.Rows[i] // 当前行是新值
				requests = append(requests, v)
			}
		}
	} else {
		// 处理 INSERT 和 DELETE 操作
		for _, row := range e.Rows {
			// 创建新的行请求对象
			v := new(model.RowRequest)
			v.RuleKey = ruleKey              // 设置规则键
			v.Action = e.Action              // 设置操作类型（INSERT 或 DELETE）
			v.Timestamp = e.Header.Timestamp // 设置时间戳
			v.Row = row                      // 设置行数据
			requests = append(requests, v)
		}
	}

	// 将处理后的请求放入队列等待批量处理
	s.queue <- requests

	// 记录处理结果
	logs.Debugf("Processed %d row requests for table %s.%s", len(requests), e.Table.Schema, e.Table.Name)

	return nil
}

// String 返回处理器的名称
// 实现 Stringer 接口，用于日志输出和调试
func (s *handler) String() string {
	return "TransferHandler"
}

// startListener 启动事件监听器
// 在独立的 goroutine 中运行，负责从队列中取出事件并批量处理
// 实现了批量处理和定时刷新机制以提高性能
func (s *handler) startListener() {
	go func() {
		// 获取配置参数
		interval := time.Duration(global.Cfg().FlushBulkInterval) // 批量刷新间隔
		bulkSize := global.Cfg().BulkSize                         // 批量大小

		// 创建定时器，用于定时刷新数据
		ticker := time.NewTicker(time.Millisecond * interval)
		defer ticker.Stop() // 确保定时器被正确关闭

		// 初始化状态变量
		lastSavedTime := time.Now()                        // 上次保存位置的时间
		requests := make([]*model.RowRequest, 0, bulkSize) // 请求缓存切片
		var current mysql.Position                         // 当前位置
		from, _ := _transferService.positionDao.Get()      // 获取起始位置

		logs.Infof("Event listener started, bulk_size=%d, flush_interval=%dms",
			bulkSize, interval)

		// 主事件处理循环
		for {
			// 初始化控制标志
			needFlush := false   // 是否需要刷新数据
			needSavePos := false // 是否需要保存位置

			// 使用 select 处理多个通道
			select {
			case v := <-s.queue: // 从队列中获取事件
				// 根据事件类型进行处理
				switch v := v.(type) {
				case model.PosRequest: // 位置更新请求
					now := time.Now()
					// 判断是否需要保存位置：强制保存 或 距离上次保存超过3秒
					if v.Force || now.Sub(lastSavedTime) > 3*time.Second {
						lastSavedTime = now // 更新保存时间
						needFlush = true    // 需要刷新当前缓存的数据
						needSavePos = true  // 需要保存位置

						// 更新当前位置
						current = mysql.Position{
							Name: v.Name,
							Pos:  v.Pos,
						}

						logs.Debugf("Position update request: %s:%d, force=%v",
							v.Name, v.Pos, v.Force)
					}

				case []*model.RowRequest: // 行数据请求
					// 将新的请求添加到缓存中
					requests = append(requests, v...)

					// 检查是否达到批量大小阈值
					needFlush = int64(len(requests)) >= global.Cfg().BulkSize

					if needFlush {
						logs.Debugf("Batch size reached: %d requests", len(requests))
					}
				}

			case <-ticker.C: // 定时器触发
				// 定时刷新，确保数据不会在缓存中停留太久
				needFlush = true
				logs.Debugf("Timer triggered flush, pending requests: %d", len(requests))

			case <-s.stop: // 接收到停止信号
				logs.Info("Event listener received stop signal")
				return
			}

			// 处理数据刷新
			if needFlush && len(requests) > 0 && _transferService.endpointEnable.Load() {
				logs.Debugf("Flushing %d requests from position %s:%d",
					len(requests), from.Name, from.Pos)

				// 调用端点处理数据
				err := _transferService.endpoint.Consume(from, requests)
				if err != nil {
					// 处理失败，禁用端点并记录错误
					_transferService.endpointEnable.Store(false)
					metrics.SetDestState(metrics.DestStateFail)
					logs.Error("Failed to consume requests: " + err.Error())

					// 异步停止数据导出
					go _transferService.stopDump()
				} else {
					logs.Debugf("Successfully consumed %d requests", len(requests))
				}

				// 重置请求切片，复用底层数组
				requests = requests[0:0]
			}

			// 处理位置保存
			if needSavePos && _transferService.endpointEnable.Load() {
				logs.Infof("Saving position %s:%d", current.Name, current.Pos)

				// 保存同步位置
				if err := _transferService.positionDao.Save(current); err != nil {
					logs.Errorf("Failed to save sync position %s: %v, closing sync", current, err)
					// 保存位置失败，关闭同步服务
					_transferService.Close()
					return
				}

				// 更新起始位置为当前位置
				from = current
				logs.Debugf("Position saved successfully, updated from position to %s:%d",
					from.Name, from.Pos)
			}
		}
	}()
}

// stopListener 停止事件监听器
// 向停止通道发送信号，优雅关闭监听器
func (s *handler) stopListener() {
	log.Println("Stopping transfer handler...")

	// 发送停止信号到停止通道
	select {
	case s.stop <- struct{}{}:
		log.Println("Stop signal sent successfully")
	default:
		log.Println("Stop channel is full, handler may already be stopping")
	}
}
