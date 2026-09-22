package pipeline

import (
	"io"
	"log/slog"
	"testing"

	"github.com/h3yow/audit-log-pipeline-go/pkg/sink"
)

// testLogger 输出事件输入、策略判定与检查点状态到测试日志（默认丢弃，
// 失败时通过 -v 或改为 os.Stderr 查看；关键状态另以 t.Logf 打印）。
func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// sinkNewReuse 为重启场景挂一个新的内存下游，返回它。
func sinkNewReuse(t *testing.T, cfg *Config) *sink.MemorySink {
	t.Helper()
	ms := sink.NewMemorySink()
	ms.SetAvailable(false)
	cfg.Sinks = map[string]sink.Sink{"audit": ms, "default": ms}
	return ms
}
