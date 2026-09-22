package storage

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// 帧布局（大端）：
//
//	magic(1)=0xA1 | version(1)=1 | bodyLen(8) | body(bodyLen) | mac(32)
//
// MAC = HMAC-SHA256(key, prevMac(32) || body)，首帧 prevMac 为全零。
// 任意字节被改写、删除整帧、乱序插入都会导致后续校验失败。
const (
	frameMagic   byte = 0xA1
	frameVersion byte = 0x01
	macSize           = sha256.Size
)

var (
	errFrameMagic   = errors.New("storage: bad frame magic")
	errFrameVersion = errors.New("storage: bad frame version")
	errFrameMAC     = errors.New("storage: frame mac mismatch")
)

// recordBody 是帧内 JSON 负载。
type recordBody struct {
	Event    json.RawMessage `json:"event"`
	Route    string          `json:"route"`
	StoredAt int64           `json:"stored_at"`
	Seq      int64           `json:"seq"`
	State    string          `json:"state,omitempty"`
	Decision string          `json:"decision,omitempty"`
}

func chainMAC(key, prev []byte, body []byte) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(prev)
	mac.Write(body)
	return mac.Sum(nil)
}

// writeFrame 追加一个帧到 w，返回本帧 MAC（成为下一帧的 prev）。
func writeFrame(w io.Writer, key, prev []byte, b recordBody) ([]byte, error) {
	body, err := json.Marshal(b)
	if err != nil {
		return nil, err
	}
	mac := chainMAC(key, prev, body)
	hdr := make([]byte, 10)
	hdr[0] = frameMagic
	hdr[1] = frameVersion
	binary.BigEndian.PutUint64(hdr[2:10], uint64(len(body)))
	if _, err := w.Write(hdr); err != nil {
		return nil, err
	}
	if _, err := w.Write(body); err != nil {
		return nil, err
	}
	if _, err := w.Write(mac); err != nil {
		return nil, err
	}
	return mac, nil
}

// readFrame 从 r 读取并校验一个帧，返回 body 与本帧 MAC。
func readFrame(r io.Reader, key, prev []byte) (recordBody, []byte, error) {
	hdr := make([]byte, 10)
	if _, err := io.ReadFull(r, hdr); err != nil {
		return recordBody{}, nil, err
	}
	if hdr[0] != frameMagic {
		return recordBody{}, nil, errFrameMagic
	}
	if hdr[1] != frameVersion {
		return recordBody{}, nil, errFrameVersion
	}
	bodyLen := binary.BigEndian.Uint64(hdr[2:10])
	if bodyLen > 64*1024*1024 {
		return recordBody{}, nil, fmt.Errorf("storage: frame body too large: %d", bodyLen)
	}
	body := make([]byte, bodyLen)
	if _, err := io.ReadFull(r, body); err != nil {
		return recordBody{}, nil, err
	}
	mac := make([]byte, macSize)
	if _, err := io.ReadFull(r, mac); err != nil {
		return recordBody{}, nil, err
	}
	want := chainMAC(key, prev, body)
	if !hmac.Equal(mac, want) {
		return recordBody{}, nil, errFrameMAC
	}
	var b recordBody
	if err := json.Unmarshal(body, &b); err != nil {
		return recordBody{}, nil, fmt.Errorf("storage: corrupt frame body: %w", err)
	}
	return b, mac, nil
}
