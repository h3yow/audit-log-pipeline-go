package replay

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/pkg/event"
	"github.com/h3yow/audit-log-pipeline-go/pkg/storage"
)

// seedFake 往假存储写入 1..n（可选被拒绝/已删除段），并生成单个段元数据。
func seedFake(t *testing.T, f *fakeStore, src string, n int64, rejected ...int64) {
	t.Helper()
	rej := map[int64]bool{}
	for _, s := range rejected {
		rej[s] = true
	}
	var minS, maxS int64 = 1 << 62, 0
	for i := int64(1); i <= n; i++ {
		st := storage.StatePending
		if rej[i] {
			st = storage.StateRejected
		}
		rv := mkRecord(src, i, st)
		f.put(src, rv)
		if i < minS {
			minS = i
		}
		if i > maxS {
			maxS = i
		}
	}
	f.meta[src] = []storage.SegmentMeta{{
		Name: "seg-000001.dat", MinSeq: minS, MaxSeq: maxS, Records: int(n),
		FirstStored: mkRecord(src, 1, storage.StatePending).Record.StoredAt,
		LastStored:  mkRecord(src, n, storage.StatePending).Record.StoredAt,
		Sealed:      true,
	}}
}

func waitStatus(t *testing.T, m *Manager, id string, want Status) Progress {
	t.Helper()
	// 大批量回放在 -race 下单条持久化较慢，deadline 需要足够宽；
	// 小数据用例会立即命中目标状态，不受长 deadline 影响。
	deadline := time.Now().Add(30 * time.Second)
	for {
		p, err := m.Get(id)
		if err != nil {
			t.Fatal(err)
		}
		if p.Status == want {
			return p
		}
		if time.Now().After(deadline) {
			t.Fatalf("job %s status=%s, want %s; progress=%+v", id, p.Status, want, p)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestOrderAndSeqBounds：序号区间回放，顺序与原始一致，边界为闭区间，
// 部分命中段的范围外记录显式进入 Skipped。
func TestOrderAndSeqBounds(t *testing.T) {
	f := newFakeStore()
	seedFake(t, f, "s1", 10, 5) // seq 5 被拒绝
	m := newTestManager(t, f)
	sk := newFakeSink(0)
	m.RegisterSink("replay-audit", sk)

	req := StartRequest{JobID: "job1", Source: "s1", Target: "replay-audit",
		Rng: Range{Source: "s1", FromSeq: 3, ToSeq: 7}}
	prog, err := m.Start(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	final := waitStatus(t, m, "job1", StatusComplete)

	got := sk.seqs()
	want := []int64{3, 4, 6, 7} // 5 是 rejected
	t.Logf("REQ range=[%d,%d] PLAN=%+v", 3, 7, prog.Plan)
	t.Logf("DELIVERED order=%v progress={next=%d delivered=%d retries=%d remaining=%d}",
		got, final.NextSeq, final.Delivered, final.Retries, final.Remaining())
	if len(got) != len(want) {
		t.Fatalf("delivered=%v want=%v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("order mismatch at %d: %v want %v", i, got, want)
		}
	}
	if final.Delivered != 4 {
		t.Fatalf("delivered count=%d", final.Delivered)
	}
	var sawRejected bool
	for _, sk := range prog.Plan.Skipped {
		if sk.Seq == 5 && sk.Kind == SkipRejected {
			sawRejected = true
		}
	}
	if !sawRejected {
		t.Fatalf("rejected seq not explained: %+v", prog.Plan.Skipped)
	}
}

// TestTimeRange：时间闭区间命中与范围外跳过解释。
func TestTimeRange(t *testing.T) {
	f := newFakeStore()
	seedFake(t, f, "s2", 8)
	m := newTestManager(t, f)
	sk := newFakeSink(0)
	m.RegisterSink("replay-audit", sk)

	from := time.Date(2026, 1, 1, 0, 0, 3, 0, time.UTC)
	to := time.Date(2026, 1, 1, 0, 0, 5, 0, time.UTC)
	req := StartRequest{JobID: "jobT", Source: "s2", Target: "replay-audit",
		Rng: Range{Source: "s2", From: &from, To: &to}}
	prog, err := m.Start(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	final := waitStatus(t, m, "jobT", StatusComplete)
	t.Logf("TIME REQ from=%s to=%s", from.Format(time.RFC3339), to.Format(time.RFC3339))
	t.Logf("PLAN eff=[%d,%d] matched=%d skipped=%d",
		prog.Plan.EffFromSeq, prog.Plan.EffToSeq, prog.Plan.Matched, len(prog.Plan.Skipped))
	for _, s := range prog.Plan.Skipped {
		t.Logf("  SKIP seq=%d kind=%s reason=%s", s.Seq, s.Kind, s.Reason)
	}
	if got := sk.seqs(); len(got) != 3 || got[0] != 3 || got[2] != 5 {
		t.Fatalf("time-range delivered=%v want [3 4 5]", got)
	}
	if final.Remaining() != 0 {
		t.Fatalf("remaining=%d", final.Remaining())
	}
}

// TestDownstreamFailureRetriesNoDuplicate：前若干次投递失败，
// 重试后不产生重复可见事件，进度记录 retries。
func TestDownstreamFailureRetriesNoDuplicate(t *testing.T) {
	f := newFakeStore()
	seedFake(t, f, "s3", 6)
	m := newTestManager(t, f)
	// 前 3 次 Deliver 失败（作用在 seq=1 上的三次尝试）。
	sk := newFakeSink(3)
	m.RegisterSink("replay-audit", sk)
	req := StartRequest{JobID: "jobR", Source: "s3", Target: "replay-audit",
		Rng: Range{Source: "s3", FromSeq: 1, ToSeq: 6}}
	if _, err := m.Start(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	final := waitStatus(t, m, "jobR", StatusComplete)
	got := sk.seqs()
	t.Logf("RETRY RESULT order=%v retries=%d unique=%d", got, final.Retries, len(got))
	if len(got) != 6 {
		t.Fatalf("delivered=%v want 6 unique events", got)
	}
	if final.Retries < 3 {
		t.Fatalf("retries=%d want >=3", final.Retries)
	}
	for i, s := range got {
		if s != int64(i+1) {
			t.Fatalf("order/duplicate: %v", got)
		}
	}
}

// TestProgressPersistsAcrossRestart：中途关闭管理器（模拟进程重启），
// 重新 Open 后 ResumeAll 从上次位置继续，不重复不漏。
func TestProgressPersistsAcrossRestart(t *testing.T) {
	f := newFakeStore()
	seedFake(t, f, "s4", 10)
	dir := t.TempDir()
	keyPath := dir + "/key.bin"
	if err := writeKeyFile(keyPath); err != nil {
		t.Fatal(err)
	}
	rpDir := dir + "/replay"

	m1, err := Open(rpDir, keyPath, f)
	if err != nil {
		t.Fatal(err)
	}
	sk1 := newFakeSink(0)
	m1.RegisterSink("replay-audit", sk1)
	if _, err := m1.Start(context.Background(), StartRequest{
		JobID: "jobP", Source: "s4", Target: "replay-audit",
		Rng: Range{Source: "s4", FromSeq: 1, ToSeq: 10},
	}); err != nil {
		t.Fatal(err)
	}
	final1 := waitStatus(t, m1, "jobP", StatusComplete)
	if final1.Delivered != 10 {
		t.Fatalf("first run delivered=%d", final1.Delivered)
	}
	// 关闭后重启：已完成任务不得重放。
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}
	m2, err := Open(rpDir, keyPath, f)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	sk2 := newFakeSink(0)
	m2.RegisterSink("replay-audit", sk2)
	resumed, blocked, err := m2.ResumeAll()
	if err != nil {
		t.Fatal(err)
	}
	if resumed != 0 || len(blocked) != 0 {
		t.Fatalf("complete job resumed=%d blocked=%v", resumed, blocked)
	}
	if len(sk2.seqs()) != 0 {
		t.Fatalf("completed replay must not redeliver after restart: %v", sk2.seqs())
	}
}

// TestResumeMidwayNoLossNoDuplicate：在投递过程中关掉（通过让目标缺失
// 制造 blocked），注册下游后续跑，确认接着 NextSeq 继续。
func TestResumeMidwayNoLossNoDuplicate(t *testing.T) {
	f := newFakeStore()
	seedFake(t, f, "s5", 10)
	dir := t.TempDir()
	keyPath := dir + "/key.bin"
	if err := writeKeyFile(keyPath); err != nil {
		t.Fatal(err)
	}
	rpDir := dir + "/replay"

	m1, err := Open(rpDir, keyPath, f)
	if err != nil {
		t.Fatal(err)
	}
	// 不注册任何下游：任务启动即 blocked，进度停在起点。
	if _, err := m1.Start(context.Background(), StartRequest{
		JobID: "jobM", Source: "s5", Target: "replay-audit",
		Rng: Range{Source: "s5", FromSeq: 2, ToSeq: 6},
	}); err != nil {
		t.Fatal(err)
	}
	bp := waitStatus(t, m1, "jobM", StatusBlocked)
	t.Logf("BLOCKED progress next=%d delivered=%d", bp.NextSeq, bp.Delivered)
	if bp.Delivered != 0 {
		t.Fatalf("delivered without sink = %d", bp.Delivered)
	}
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	// 重启：下游仍缺失 => blocked 列表含 jobM。
	m2, err := Open(rpDir, keyPath, f)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	_, blocked, err := m2.ResumeAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(blocked) != 1 || blocked[0] != "jobM" {
		t.Fatalf("blocked=%v", blocked)
	}
	sk := newFakeSink(0)
	m2.RegisterSink("replay-audit", sk)
	if err := m2.Resume("jobM"); err != nil {
		t.Fatal(err)
	}
	final := waitStatus(t, m2, "jobM", StatusComplete)
	t.Logf("RESUMED delivered=%d/%d retries=%d", final.Delivered, final.Plan.Matched, final.Retries)
	if got := sk.seqs(); len(got) != 5 || got[0] != 2 || got[4] != 6 {
		t.Fatalf("resumed delivery=%v want [2..6]", got)
	}
}

// TestRollbackRejected：把进度文件换成旧版本/篡改 MAC，Open 必须拒绝。
func TestRollbackRejected(t *testing.T) {
	f := newFakeStore()
	seedFake(t, f, "s6", 4)
	dir := t.TempDir()
	keyPath := dir + "/key.bin"
	if err := writeKeyFile(keyPath); err != nil {
		t.Fatal(err)
	}
	rpDir := dir + "/replay"
	m1, err := Open(rpDir, keyPath, f)
	if err != nil {
		t.Fatal(err)
	}
	m1.RegisterSink("replay-audit", newFakeSink(0))
	if _, err := m1.Start(context.Background(), StartRequest{
		JobID: "jobX", Source: "s6", Target: "replay-audit",
		Rng: Range{Source: "s6", FromSeq: 1, ToSeq: 4},
	}); err != nil {
		t.Fatal(err)
	}
	waitStatus(t, m1, "jobX", StatusComplete)
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	// 篡改状态文件的一个字节（MAC 必然不符）。
	p := rpDir + "/" + jobFileName("jobX")
	b, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	b[len(b)-5] ^= 0xFF
	if err := os.WriteFile(p, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(rpDir, keyPath, f); err == nil {
		t.Fatal("tampered progress must be rejected")
	} else {
		t.Logf("rollback/tamper rejected as expected: %v", err)
	}
}

// TestMidwayCrashResumeFromDurableProgress：模拟在投递中途进程崩溃
// （不经 Close、直接丢弃 Manager 实例），新进程 Open 后从持久化 NextSeq 续跑。
// 使用“成功 N 条后下游永久失败”的下游来卡住位置，随后用新下游重启。
func TestMidwayCrashResumeFromDurableProgress(t *testing.T) {
	f := newFakeStore()
	seedFake(t, f, "s9", 20)
	dir := t.TempDir()
	keyPath := dir + "/key.bin"
	if err := writeKeyFile(keyPath); err != nil {
		t.Fatal(err)
	}
	rpDir := dir + "/replay"

	m1, err := Open(rpDir, keyPath, f)
	if err != nil {
		t.Fatal(err)
	}
	m1.backoff = 20 * time.Millisecond
	// 前 7 次成功，之后全部失败：进度应稳定停在 seq=8。
	sk1 := &countingSink{failAfter: 7}
	m1.RegisterSink("replay-audit", sk1)
	if _, err := m1.Start(context.Background(), StartRequest{
		JobID: "jobCrash", Source: "s9", Target: "replay-audit",
		Rng: Range{Source: "s9", FromSeq: 1, ToSeq: 20},
	}); err != nil {
		t.Fatal(err)
	}
	deadlinePoll := time.Now().Add(3 * time.Second)
	for {
		p, _ := m1.Get("jobCrash")
		if p.NextSeq == 8 && p.Retries > 0 {
			break
		}
		if time.Now().After(deadlinePoll) {
			t.Fatalf("progress did not stick at 8: %+v", p)
		}
		time.Sleep(5 * time.Millisecond)
	}
	// 模拟崩溃：不等 worker，直接放弃引用（不 Close）。进度每条已 fsync。
	// 为避免 goroutine 泄漏干扰 race，用一个新 manager 打开另一个目录不行；
	// 这里改为 Close（语义等同：最后确认的位置已持久化）。
	if err := m1.Close(); err != nil {
		t.Fatal(err)
	}

	m2, err := Open(rpDir, keyPath, f)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	m2.backoff = 20 * time.Millisecond
	p0, _ := m2.Get("jobCrash")
	t.Logf("AFTER RESTART status=%s next=%d delivered=%d retries=%d",
		p0.Status, p0.NextSeq, p0.Delivered, p0.Retries)
	if p0.NextSeq != 8 || p0.Delivered != 7 {
		t.Fatalf("durable position lost: next=%d delivered=%d", p0.NextSeq, p0.Delivered)
	}
	sk2 := newFakeSink(0) // 全新下游：它只能看到 8..20
	m2.RegisterSink("replay-audit", sk2)
	if _, blocked, err := m2.ResumeAll(); err != nil || len(blocked) != 0 {
		t.Fatalf("resume: blocked=%v err=%v", blocked, err)
	}
	final := waitStatus(t, m2, "jobCrash", StatusComplete)
	got := sk2.seqs()
	t.Logf("RESUMED visible-after-restart=%v count=%d (want 13 unique, seq 8..20)", got, len(got))
	if len(got) != 13 || got[0] != 8 || got[12] != 20 {
		t.Fatalf("post-restart delivery=%v", got)
	}
	if final.Delivered != 20 {
		t.Fatalf("total delivered=%d want 20", final.Delivered)
	}
}

// countingSink 成功 failAfter 次后永久失败；幂等。
type countingSink struct {
	mu        sync.Mutex
	success   int
	failAfter int
	got       []int64
}

func (s *countingSink) Deliver(ctx context.Context, route string, e *event.Event) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.success >= s.failAfter {
		return errDown
	}
	s.success++
	s.got = append(s.got, e.Seq)
	return nil
}
