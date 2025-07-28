package service

import (
	"fmt"
	"testing"
	"time"

	"github.com/siddontang/go-mysql/canal"
)

func TestRobustCanal_Basic(t *testing.T) {
	// 创建测试配置
	cfg := canal.NewDefaultConfig()
	cfg.Addr = "127.0.0.1:3306"
	cfg.User = "root"
	cfg.Password = "password"
	cfg.Charset = "utf8"
	cfg.ServerID = 1001

	// 创建RobustCanal实例
	rc, err := NewRobustCanal(cfg)
	if err != nil {
		t.Fatalf("Failed to create RobustCanal: %v", err)
	}
	defer rc.Close()

	// 测试健康状态
	if rc.IsHealthy() {
		t.Log("RobustCanal is initially healthy")
	}

	// 测试启动
	if err := rc.Start(); err != nil {
		t.Logf("Failed to start RobustCanal (expected in test environment): %v", err)
	}

	// 等待一段时间让健康检查运行
	time.Sleep(2 * time.Second)

	// 测试关闭
	rc.Close()
	t.Log("RobustCanal closed successfully")
}

func TestRobustCanal_PositionValidation(t *testing.T) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = "127.0.0.1:3306"
	cfg.User = "root"
	cfg.Password = "password"
	cfg.Charset = "utf8"
	cfg.ServerID = 1001

	rc, err := NewRobustCanal(cfg)
	if err != nil {
		t.Fatalf("Failed to create RobustCanal: %v", err)
	}
	defer rc.Close()

	// 测试binlog编号提取
	testCases := []struct {
		filename string
		expected int
	}{
		{"mysql-bin.000001", 1},
		{"mysql-bin.000123", 123},
		{"binlog.000456", 456},
		{"invalid", 0},
	}

	for _, tc := range testCases {
		result := rc.extractBinlogNumber(tc.filename)
		if result != tc.expected {
			t.Errorf("extractBinlogNumber(%s) = %d, expected %d", tc.filename, result, tc.expected)
		}
	}
}

func TestRobustCanal_ErrorHandling(t *testing.T) {
	cfg := canal.NewDefaultConfig()
	cfg.Addr = "127.0.0.1:3306"
	cfg.User = "root"
	cfg.Password = "password"
	cfg.Charset = "utf8"
	cfg.ServerID = 1001

	rc, err := NewRobustCanal(cfg)
	if err != nil {
		t.Fatalf("Failed to create RobustCanal: %v", err)
	}
	defer rc.Close()

	// 模拟序列号错误
	testError := fmt.Errorf("invalid sequence number")
	rc.handleSequenceError(testError)

	// 检查错误是否被正确记录
	if rc.LastError() == nil {
		t.Error("Expected error to be recorded")
	}

	if rc.IsHealthy() {
		t.Error("Expected RobustCanal to be unhealthy after error")
	}
}
