package event

import "time"

// DecisionKind 描述事件的处理结论类别，彼此可区分、不可静默丢弃。
type DecisionKind string

const (
	DecisionAccepted  DecisionKind = "accepted"  // 已接受并路由
	DecisionDuplicate DecisionKind = "duplicate" // 重复投递，跳过但显式记录
	DecisionMalformed DecisionKind = "malformed" // 格式非法
	DecisionRejected  DecisionKind = "rejected"  // 命中拒绝策略
)

// Decision 是单个事件（或一段非法输入）的可解释处理结论。
type Decision struct {
	Kind      DecisionKind
	EventID   string
	Source    string
	Seq       int64
	Reason    string
	Rule      string
	Route     string
	Timestamp time.Time
}
