package storage

import "github.com/h3yow/audit-log-pipeline-go/pkg/event"

// DeliveryState 标记一条已落盘记录的投递生命周期。
type DeliveryState string

const (
	StatePending   DeliveryState = "pending"   // 已落盘，待投递
	StateDelivered DeliveryState = "delivered" // 已确认投递并越过检查点
	StateRejected  DeliveryState = "rejected"  // 命中拒绝策略：留存但不投递
)

// Record 是一条已落盘的审计记录。
type Record struct {
	Event    *event.Event
	Route    string
	StoredAt int64 // unix 秒
	State    DeliveryState
	Decision string // 命中策略名/拒绝原因，便于解释
}

// RecordView 是扫描读出的记录视图。
type RecordView struct {
	Record Record
	Seq    int64 // 存储侧单调序号（即事件来源内序号）
}
