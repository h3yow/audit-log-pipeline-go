package event

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Event 是接入审计管道的原始事件。
type Event struct {
	ID        string          `json:"id"`
	Source    string          `json:"source"`
	Seq       int64           `json:"seq"`
	Type      string          `json:"type"`
	Subject   string          `json:"subject"`
	Action    string          `json:"action"`
	Resource  string          `json:"resource"`
	Timestamp time.Time       `json:"timestamp"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

// rawEvent 用于严格解析：时间戳必须是合法 RFC3339 字符串。
type rawEvent struct {
	ID        string          `json:"id"`
	Source    string          `json:"source"`
	Seq       int64           `json:"seq"`
	Type      string          `json:"type"`
	Subject   string          `json:"subject"`
	Action    string          `json:"action"`
	Resource  string          `json:"resource"`
	Timestamp string          `json:"timestamp"`
	Payload   json.RawMessage `json:"payload,omitempty"`
}

var (
	// ErrMalformed 表示输入不是合法的事件编码。
	ErrMalformed = errors.New("event: malformed input")
	// ErrInvalid 表示字段不满足完整性要求。
	ErrInvalid = errors.New("event: invalid field")
)

// ParseEvent 从 JSON 负载严格解析事件：
// 拒绝空输入、非法 JSON、未知字段、非法时间戳。
func ParseEvent(data []byte) (*Event, error) {
	if len(bytes.TrimSpace(data)) == 0 {
		return nil, fmt.Errorf("%w: empty body", ErrMalformed)
	}
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	var raw rawEvent
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrMalformed, err)
	}
	if dec.More() {
		return nil, fmt.Errorf("%w: multiple JSON values", ErrMalformed)
	}
	ts, err := time.Parse(time.RFC3339, raw.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("%w: timestamp: %v", ErrMalformed, err)
	}
	e := &Event{
		ID:        raw.ID,
		Source:    raw.Source,
		Seq:       raw.Seq,
		Type:      raw.Type,
		Subject:   raw.Subject,
		Action:    raw.Action,
		Resource:  raw.Resource,
		Timestamp: ts,
		Payload:   raw.Payload,
	}
	if err := e.Validate(); err != nil {
		return nil, err
	}
	return e, nil
}

// Validate 校验事件字段完整性：
// id/source/type/action 非空，seq 必须为正数，时间戳非零。
func (e *Event) Validate() error {
	switch {
	case e.ID == "":
		return fmt.Errorf("%w: id is required", ErrInvalid)
	case e.Source == "":
		return fmt.Errorf("%w: source is required", ErrInvalid)
	case e.Seq <= 0:
		return fmt.Errorf("%w: seq must be positive", ErrInvalid)
	case e.Type == "":
		return fmt.Errorf("%w: type is required", ErrInvalid)
	case e.Action == "":
		return fmt.Errorf("%w: action is required", ErrInvalid)
	case e.Timestamp.IsZero():
		return fmt.Errorf("%w: timestamp is required", ErrInvalid)
	}
	return nil
}

// Marshal 规范编码（确定性 JSON），供落盘与测试断言使用。
func (e *Event) Marshal() ([]byte, error) {
	return json.Marshal(e)
}
