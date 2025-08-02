package global

import (
	"runtime"
	"strconv"
	"syscall"
	"time"

	"go-mysql-transfer/util/logs"

	sidlog "github.com/siddontang/go-log/log"
)

var (
	_pid         int
	_leaderFlag  bool
	_leaderNode  string
	_currentNode string
	_bootTime    time.Time
)

func SetLeaderFlag(flag bool) {
	_leaderFlag = flag
}

func IsLeader() bool {
	return _leaderFlag
}

func SetLeaderNode(leader string) {
	_leaderNode = leader
}

func LeaderNode() string {
	return _leaderNode
}

func CurrentNode() string {
	return _currentNode
}

func IsFollower() bool {
	return !_leaderFlag
}

func BootTime() time.Time {
	return _bootTime
}

func Initialize(configPath string) error {
	if err := initConfig(configPath); err != nil {
		return err
	}
	runtime.GOMAXPROCS(_config.Maxprocs)

	// 初始化global logger
	if err := logs.Initialize(_config.LoggerConfig); err != nil {
		return err
	}

	streamHandler, err := sidlog.NewStreamHandler(logs.Writer())
	if err != nil {
		return err
	}
	agent := sidlog.New(streamHandler, sidlog.Ltime|sidlog.Lfile|sidlog.Llevel)

	// 根据配置设置Canal日志级别，减少不必要的DDL变更日志
	canalLogLevel := getCanalLogLevel(_config.CanalLogLevel)
	agent.SetLevel(canalLogLevel)
	sidlog.SetDefaultLogger(agent)

	_bootTime = time.Now()
	_pid = syscall.Getpid()

	if _config.IsCluster() {
		if _config.EnableWebAdmin {
			_currentNode = _config.Cluster.BindIp + ":" + strconv.Itoa(_config.WebAdminPort)
		} else {
			_currentNode = _config.Cluster.BindIp + ":" + strconv.Itoa(_pid)
		}
	}

	logs.Infof("process id: %d", _pid)
	logs.Infof("GOMAXPROCS: %d", _config.Maxprocs)
	logs.Infof("source: %s(%s)", _config.Flavor, _config.Addr)
	logs.Infof("destination: %s", _config.Destination())

	return nil
}

// getCanalLogLevel 将字符串日志级别转换为Canal日志级别
func getCanalLogLevel(level string) sidlog.Level {
	switch level {
	case "debug":
		return sidlog.LevelDebug
	case "info":
		return sidlog.LevelInfo
	case "warn":
		return sidlog.LevelWarn
	case "error":
		return sidlog.LevelError
	default:
		return sidlog.LevelWarn // 默认为warn级别
	}
}
