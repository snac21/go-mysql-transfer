package service

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	_ "github.com/go-sql-driver/mysql"

	"go-mysql-transfer/util/logs"
)

type RobustCanal struct {
	cfg    *canal.Config
	canal  *canal.Canal
	syncer *replication.BinlogSyncer
	db     *sql.DB

	// 连接状态管理
	mu            sync.RWMutex
	isHealthy     bool
	lastError     error
	lastErrorTime time.Time

	// 上下文控制
	ctx    context.Context
	cancel context.CancelFunc

	// 健康检查
	healthCheckInterval time.Duration
	connectionTimeout   time.Duration

	// 事件处理器
	eventHandler canal.EventHandler
}

func NewRobustCanal(cfg *canal.Config) (*RobustCanal, error) {
	ctx, cancel := context.WithCancel(context.Background())
	rc := &RobustCanal{
		cfg:                 cfg,
		ctx:                 ctx,
		cancel:              cancel,
		isHealthy:           false,
		healthCheckInterval: 30 * time.Second,
		connectionTimeout:   10 * time.Second,
	}

	// 建立数据库连接用于健康检查
	dsn := fmt.Sprintf("%s:%s@tcp(%s)/?charset=%s&timeout=%s",
		cfg.User, cfg.Password, cfg.Addr, cfg.Charset, rc.connectionTimeout)

	var err error
	rc.db, err = sql.Open("mysql", dsn)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("failed to create database connection: %v", err)
	}

	rc.db.SetMaxOpenConns(1)
	rc.db.SetMaxIdleConns(1)
	rc.db.SetConnMaxLifetime(time.Hour)

	return rc, nil
}

// 核心方法：检测并修复连接状态
func (rc *RobustCanal) detectAndFixConnectionState() error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	// 1. 检查MySQL连接是否正常
	if !rc.isConnectionHealthy() {
		return fmt.Errorf("mysql connection is not healthy")
	}

	// 2. 检查binlog位置是否有效
	if !rc.isBinlogPositionValid() {
		return fmt.Errorf("binlog position is invalid")
	}

	// 3. 检查canal状态
	if rc.canal != nil && !rc.isCanalHealthy() {
		rc.closeCanal()
		return fmt.Errorf("canal is not healthy, closed")
	}

	return nil
}

func (rc *RobustCanal) isConnectionHealthy() bool {
	ctx, cancel := context.WithTimeout(rc.ctx, rc.connectionTimeout)
	defer cancel()

	var result int
	err := rc.db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	return err == nil && result == 1
}

func (rc *RobustCanal) isBinlogPositionValid() bool {
	ctx, cancel := context.WithTimeout(rc.ctx, rc.connectionTimeout)
	defer cancel()

	var file string
	var position uint32
	err := rc.db.QueryRowContext(ctx, "SHOW MASTER STATUS").Scan(&file, &position, nil, nil, nil)
	if err != nil {
		return false
	}

	// 检查当前canal的位置是否还有效
	if rc.canal != nil {
		currentPos := rc.canal.SyncedPosition()
		if currentPos.Name != "" {
			// 验证位置是否合理（不能超过当前master位置太多）
			return rc.isPositionReasonable(currentPos, mysql.Position{Name: file, Pos: position})
		}
	}

	return true
}

func (rc *RobustCanal) isPositionReasonable(current, master mysql.Position) bool {
	// 如果binlog文件不同，需要更仔细的检查
	if current.Name != master.Name {
		// 提取binlog文件的序号进行比较
		currentNum := rc.extractBinlogNumber(current.Name)
		masterNum := rc.extractBinlogNumber(master.Name)
		// 当前位置不应该超过master太多
		return currentNum <= masterNum
	}

	// 同一个文件内，位置不应该超过master位置
	return current.Pos <= master.Pos
}

func (rc *RobustCanal) extractBinlogNumber(filename string) int {
	parts := strings.Split(filename, ".")
	if len(parts) < 2 {
		return 0
	}
	var num int
	fmt.Sscanf(parts[len(parts)-1], "%d", &num)
	return num
}

func (rc *RobustCanal) isCanalHealthy() bool {
	if rc.canal == nil {
		return false
	}

	// 检查canal是否正常工作
	// 我们通过检查是否能获取当前位置来判断canal的健康状态
	defer func() {
		if r := recover(); r != nil {
			// 如果调用canal方法时发生panic，说明canal不健康
			logs.Warnf("Canal health check panic: %v", r)
		}
	}()

	// 尝试获取当前同步位置，如果能正常获取说明canal工作正常
	pos := rc.canal.SyncedPosition()
	// 如果位置为空，可能是canal还没开始同步，这也是正常的
	_ = pos

	return true
}

// 安全地关闭canal
func (rc *RobustCanal) closeCanal() {
	if rc.canal != nil {
		rc.canal.Close()
		rc.canal = nil
	}
	if rc.syncer != nil {
		rc.syncer.Close()
		rc.syncer = nil
	}
}

// 创建新的canal连接
func (rc *RobustCanal) createCanal() error {
	var err error
	// 创建新的canal实例
	rc.canal, err = canal.NewCanal(rc.cfg)
	if err != nil {
		return fmt.Errorf("failed to create canal: %v", err)
	}

	// 设置事件处理器
	if rc.eventHandler != nil {
		rc.canal.SetEventHandler(&sequenceErrorHandler{
			onSequenceError: rc.handleSequenceError,
			originalHandler: rc.eventHandler,
		})
	}

	return nil
}

// AddDumpTables 添加需要dump的表
func (rc *RobustCanal) AddDumpTables(db string, tables ...string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.canal != nil {
		rc.canal.AddDumpTables(db, tables...)
	}
}

// AddDumpDatabases 添加需要dump的数据库
func (rc *RobustCanal) AddDumpDatabases(dbs ...string) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.canal != nil {
		rc.canal.AddDumpDatabases(dbs...)
	}
}

// Execute 执行SQL查询
func (rc *RobustCanal) Execute(cmd string, args ...interface{}) (*mysql.Result, error) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if rc.canal != nil {
		return rc.canal.Execute(cmd, args...)
	}
	return nil, fmt.Errorf("canal is not initialized")
}

// GetTable 获取表信息
func (rc *RobustCanal) GetTable(db string, table string) (*schema.Table, error) {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if rc.canal != nil {
		return rc.canal.GetTable(db, table)
	}
	return nil, fmt.Errorf("canal is not initialized")
}

// 序列号错误处理器
type sequenceErrorHandler struct {
	onSequenceError func(error)
	originalHandler canal.EventHandler
}

func (h *sequenceErrorHandler) OnRow(e *canal.RowsEvent) error {
	err := h.originalHandler.OnRow(e)
	if err != nil && strings.Contains(err.Error(), "invalid sequence") {
		h.onSequenceError(err)
		return nil // 不向上传播错误，让系统自动恢复
	}
	return err
}

func (h *sequenceErrorHandler) String() string {
	return "sequenceErrorHandler"
}

func (h *sequenceErrorHandler) OnRotate(header *replication.EventHeader, e *replication.RotateEvent) error {
	if rotateHandler, ok := h.originalHandler.(interface {
		OnRotate(*replication.EventHeader, *replication.RotateEvent) error
	}); ok {
		return rotateHandler.OnRotate(header, e)
	}
	return nil
}

func (h *sequenceErrorHandler) OnTableChanged(header *replication.EventHeader, schema string, table string) error {
	if tableHandler, ok := h.originalHandler.(interface {
		OnTableChanged(*replication.EventHeader, string, string) error
	}); ok {
		return tableHandler.OnTableChanged(header, schema, table)
	}
	return nil
}

func (h *sequenceErrorHandler) OnDDL(header *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	if ddlHandler, ok := h.originalHandler.(interface {
		OnDDL(*replication.EventHeader, mysql.Position, *replication.QueryEvent) error
	}); ok {
		return ddlHandler.OnDDL(header, nextPos, queryEvent)
	}
	return nil
}

func (h *sequenceErrorHandler) OnXID(header *replication.EventHeader, nextPos mysql.Position) error {
	if xidHandler, ok := h.originalHandler.(interface {
		OnXID(*replication.EventHeader, mysql.Position) error
	}); ok {
		return xidHandler.OnXID(header, nextPos)
	}
	return nil
}

func (h *sequenceErrorHandler) OnGTID(header *replication.EventHeader, gtid mysql.BinlogGTIDEvent) error {
	if gtidHandler, ok := h.originalHandler.(interface {
		OnGTID(*replication.EventHeader, mysql.BinlogGTIDEvent) error
	}); ok {
		return gtidHandler.OnGTID(header, gtid)
	}
	return nil
}

func (h *sequenceErrorHandler) OnPosSynced(header *replication.EventHeader, pos mysql.Position, set mysql.GTIDSet, force bool) error {
	if posHandler, ok := h.originalHandler.(interface {
		OnPosSynced(*replication.EventHeader, mysql.Position, mysql.GTIDSet, bool) error
	}); ok {
		return posHandler.OnPosSynced(header, pos, set, force)
	}
	return nil
}

func (h *sequenceErrorHandler) OnRowsQueryEvent(e *replication.RowsQueryEvent) error {
	if rowsQueryHandler, ok := h.originalHandler.(interface {
		OnRowsQueryEvent(*replication.RowsQueryEvent) error
	}); ok {
		return rowsQueryHandler.OnRowsQueryEvent(e)
	}
	return nil
}

// 处理序列号错误
func (rc *RobustCanal) handleSequenceError(err error) {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.isHealthy = false
	rc.lastError = err
	rc.lastErrorTime = time.Now()

	// 记录错误但不立即重启，等待健康检查处理
	logs.Warnf("Sequence error detected: %v, will be handled by health check", err)
}

// 主启动方法
func (rc *RobustCanal) Start() error {
	// 启动健康检查goroutine
	go rc.healthCheckLoop()

	// 初始化canal
	return rc.ensureCanalRunning()
}

// 确保canal正在运行
func (rc *RobustCanal) ensureCanalRunning() error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.canal != nil && rc.isCanalHealthy() {
		return nil
	}

	// 清理旧连接
	rc.closeCanal()

	// 等待一小段时间让连接完全清理
	time.Sleep(2 * time.Second)

	// 创建新连接
	if err := rc.createCanal(); err != nil {
		return err
	}

	rc.isHealthy = true
	return nil
}

// 健康检查循环
func (rc *RobustCanal) healthCheckLoop() {
	ticker := time.NewTicker(rc.healthCheckInterval)
	defer ticker.Stop()

	for {
		select {
		case <-rc.ctx.Done():
			return
		case <-ticker.C:
			if err := rc.detectAndFixConnectionState(); err != nil {
				logs.Infof("Health check failed: %v, attempting to fix...", err)
				if err := rc.ensureCanalRunning(); err != nil {
					logs.Errorf("Failed to fix canal: %v", err)
				} else {
					logs.Info("Canal fixed successfully")
				}
			}
		}
	}
}

// 获取当前状态
func (rc *RobustCanal) IsHealthy() bool {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.isHealthy
}

func (rc *RobustCanal) LastError() error {
	rc.mu.RLock()
	defer rc.mu.RUnlock()
	return rc.lastError
}

// 设置事件处理器
func (rc *RobustCanal) SetEventHandler(handler canal.EventHandler) {
	rc.eventHandler = handler
}

// RunFrom 从指定位置开始运行
func (rc *RobustCanal) RunFrom(pos mysql.Position) error {
	rc.mu.Lock()
	defer rc.mu.Unlock()

	if rc.canal == nil {
		return fmt.Errorf("canal is not initialized")
	}

	// 启动canal
	go func() {
		defer func() {
			if r := recover(); r != nil {
				err := fmt.Errorf("canal panic: %v", r)
				rc.handleSequenceError(err)
			}
		}()

		logs.Infof("Starting canal from position: %s %d", pos.Name, pos.Pos)
		if err := rc.canal.RunFrom(pos); err != nil {
			rc.handleSequenceError(fmt.Errorf("canal run error: %v", err))
		}
	}()

	return nil
}

// SyncedPosition 获取当前同步位置
func (rc *RobustCanal) SyncedPosition() mysql.Position {
	rc.mu.RLock()
	defer rc.mu.RUnlock()

	if rc.canal != nil {
		return rc.canal.SyncedPosition()
	}
	return mysql.Position{}
}

// 优雅关闭
func (rc *RobustCanal) Close() {
	rc.cancel()
	rc.mu.Lock()
	defer rc.mu.Unlock()

	rc.closeCanal()
	if rc.db != nil {
		rc.db.Close()
	}
}
