package checkpoint

import (
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func setupCP(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		t.Fatal(err)
	}
	kf := filepath.Join(dir, "key.bin")
	if err := os.WriteFile(kf, key, 0o600); err != nil {
		t.Fatal(err)
	}
	return filepath.Join(dir, "cp"), kf
}

func TestAdvanceMonotonic(t *testing.T) {
	dir, kf := setupCP(t)
	m, err := Open(dir, kf)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.Advance("s1", 1, 10); err != nil {
		t.Fatal(err)
	}
	if err := m.Advance("s1", 5, 20); err != nil {
		t.Fatal(err)
	}
	if err := m.Advance("s1", 3, 30); !errors.Is(err, ErrRollback) {
		t.Fatalf("rollback err = %v, want ErrRollback", err)
	}
	if err := m.Advance("s1", 5, 40); !errors.Is(err, ErrRollback) {
		t.Fatalf("equal watermark err = %v, want ErrRollback", err)
	}
	st, _ := m.Get("s1")
	if st.Watermark != 5 {
		t.Fatalf("watermark = %d", st.Watermark)
	}
	if err := m.Close(); err != nil {
		t.Fatal(err)
	}
	// 重启后水位保持
	m2, err := Open(dir, kf)
	if err != nil {
		t.Fatal(err)
	}
	defer m2.Close()
	st2, _ := m2.Get("s1")
	if st2.Watermark != 5 {
		t.Fatalf("watermark after restart = %d", st2.Watermark)
	}
}

func TestRestoreOldCopyRejected(t *testing.T) {
	dir, kf := setupCP(t)
	m, _ := Open(dir, kf)
	_ = m.Advance("s1", 1, 10)
	// 保存水位=1 的状态文件副本
	cpPath := filepath.Join(dir, "7331.cp")
	oldCopy, _ := os.ReadFile(cpPath)
	_ = m.Advance("s1", 2, 20)
	_ = m.Close()
	// 用旧副本覆盖（模拟检查点回退/换复件）
	if err := os.WriteFile(cpPath, oldCopy, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, kf); !errors.Is(err, ErrRollback) {
		t.Fatalf("restored old copy err = %v, want ErrRollback", err)
	}
}

func TestAnchorTamperRejected(t *testing.T) {
	dir, kf := setupCP(t)
	m, _ := Open(dir, kf)
	_ = m.Advance("s1", 1, 10)
	_ = m.Close()
	ap := filepath.Join(dir, anchorsName)
	b, _ := os.ReadFile(ap)
	// 翻转文件内第一个十六进制字符（锚点/MAC 内容），破坏绑定。
	for i, c := range b {
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') {
			if c == '0' {
				b[i] = '1'
			} else {
				b[i] = '0'
			}
			break
		}
	}
	if err := os.WriteFile(ap, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, kf); !errors.Is(err, ErrRollback) {
		t.Fatalf("anchor tamper err = %v, want ErrRollback", err)
	}
}
