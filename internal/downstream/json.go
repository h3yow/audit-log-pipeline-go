package downstream

import "encoding/json"

// parseRecordID extracts the record id used as the downstream
// idempotency key. The pipeline always includes record_id in payloads.
func parseRecordID(b []byte) int64 {
	var probe struct {
		RecordID int64 `json:"record_id"`
	}
	_ = json.Unmarshal(b, &probe)
	return probe.RecordID
}
