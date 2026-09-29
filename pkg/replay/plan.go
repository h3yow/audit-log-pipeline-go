package replay

import (
	"fmt"
	"time"

	"github.com/h3yow/audit-log-pipeline-go/pkg/storage"
)

// buildPlan 对请求范围做静态判定（快照语义）：
//  1. 校验请求：来源非空、至少一个边界、区间合法；
//  2. 用清单快照确定实际扫描的序号闭区间；
//  3. 顺序扫描该区间（流式，不整段载入内存），逐条判定
//     命中 / 跳过原因，并记录请求序号区间内的空洞（已清理 vs 从未存在）；
//  4. 返回完整、可解释的判定结果，绝不静默跳过任何记录。
func (m *Manager) buildPlan(req StartRequest) (Plan, error) {
	rng := req.Rng
	if req.Source == "" {
		return Plan{}, fmt.Errorf("%w: source required", ErrInvalidRequest)
	}
	if rng.Source == "" {
		rng.Source = req.Source
	}
	if rng.Source != req.Source {
		return Plan{}, fmt.Errorf("%w: range source mismatch", ErrInvalidRequest)
	}
	if rng.FromSeq < 0 || rng.ToSeq < 0 {
		return Plan{}, fmt.Errorf("%w: negative seq bound", ErrInvalidRequest)
	}
	if rng.FromSeq > 0 && rng.ToSeq > 0 && rng.FromSeq > rng.ToSeq {
		return Plan{}, fmt.Errorf("%w: from_seq > to_seq", ErrInvalidRequest)
	}
	if rng.From != nil && rng.To != nil && rng.From.After(*rng.To) {
		return Plan{}, fmt.Errorf("%w: from time after to time", ErrInvalidRequest)
	}
	if rng.FromSeq <= 0 && rng.ToSeq <= 0 && rng.From == nil && rng.To == nil {
		return Plan{}, fmt.Errorf("%w: at least one range bound required", ErrInvalidRequest)
	}

	snap, err := m.store.ReplaySnapshot(req.Source)
	if err != nil {
		return Plan{}, err
	}

	// 1) 用段元数据推导实际扫描的序号闭区间 [effFrom, effTo]，
	//    并依据 StoredAt（unix 秒）剪枝明显不可能命中时间条件的已封存段。
	effFrom, effTo := int64(0), int64(0)
	for _, seg := range snap.Segments {
		if seg.Deleted || seg.Records == 0 || seg.MaxSeq == 0 {
			continue
		}
		if !segmentCouldMatch(seg, rng) {
			continue
		}
		lo := seg.MinSeq
		if rng.FromSeq > 0 && lo < rng.FromSeq {
			lo = rng.FromSeq
		}
		hi := seg.MaxSeq
		if rng.ToSeq > 0 && hi > rng.ToSeq {
			hi = rng.ToSeq
		}
		if lo > hi {
			continue
		}
		if effFrom == 0 || lo < effFrom {
			effFrom = lo
		}
		if hi > effTo {
			effTo = hi
		}
	}

	plan := Plan{
		Source:     req.Source,
		Requested:  rng,
		EffFromSeq: effFrom,
		EffToSeq:   effTo,
		CreatedAt:  time.Now().UTC(),
	}
	if effFrom == 0 {
		plan.Note = "no retained segment intersects the requested range"
		return plan, nil
	}

	// 2) 流式扫描 [effFrom, effTo] 做逐条判定。
	//    缺失序号用 deleted 段信息判定为 purged，否则为 missing。
	deletedIntervals := buildDeletedIntervals(snap, req.Source)
	lastSeen := effFrom - 1
	err = m.store.Scan(req.Source, effFrom, effTo, func(rv storage.RecordView) bool {
		seq := rv.Seq
		// seq 之前的空洞：(lastSeen, seq)
		if seq > lastSeen+1 {
			plan.Gaps = append(plan.Gaps, classifyGap(lastSeen+1, seq-1, deletedIntervals))
		}
		lastSeen = seq

		// 序号边界（含“只有一部分命中”的段）。
		if rng.FromSeq > 0 && seq < rng.FromSeq {
			plan.Skipped = append(plan.Skipped, Skipped{Seq: seq, Kind: SkipBeforeRange,
				Reason: fmt.Sprintf("seq %d < from_seq %d (partially matched segment)", seq, rng.FromSeq)})
			return true
		}
		if rng.ToSeq > 0 && seq > rng.ToSeq {
			plan.Skipped = append(plan.Skipped, Skipped{Seq: seq, Kind: SkipAfterRange,
				Reason: fmt.Sprintf("seq %d > to_seq %d (partially matched segment)", seq, rng.ToSeq)})
			return true
		}
		// 接入时被拒绝：留存但不投递（含回放，语义一致且可解释）。
		if rv.Record.State == storage.StateRejected {
			plan.Skipped = append(plan.Skipped, Skipped{Seq: seq, Kind: SkipRejected,
				Reason: emptyReason(rv.Record.Decision, "event was rejected at ingest")})
			return true
		}
		// 时间边界（事件时间戳，含边界时刻）。
		ts := rv.Record.Event.Timestamp
		if rng.From != nil && ts.Before(*rng.From) {
			plan.Skipped = append(plan.Skipped, Skipped{Seq: seq, Kind: SkipOutsideTime,
				Reason: fmt.Sprintf("timestamp %s before from %s", ts.Format(time.RFC3339), rng.From.Format(time.RFC3339))})
			return true
		}
		if rng.To != nil && ts.After(*rng.To) {
			plan.Skipped = append(plan.Skipped, Skipped{Seq: seq, Kind: SkipOutsideTime,
				Reason: fmt.Sprintf("timestamp %s after to %s", ts.Format(time.RFC3339), rng.To.Format(time.RFC3339))})
			return true
		}
		plan.Matched++
		return true
	})
	if err != nil {
		return Plan{}, err
	}
	// 尾部空洞 (lastSeen, effTo]：这一段可能是被清理或从未存在。
	if lastSeen < effTo {
		plan.Gaps = append(plan.Gaps, classifyGap(lastSeen+1, effTo, deletedIntervals))
	}
	plan.Note = fmt.Sprintf("range resolved to seq [%d,%d]; matched=%d gaps=%d skipped=%d",
		effFrom, effTo, plan.Matched, len(plan.Gaps), len(plan.Skipped))
	return plan, nil
}

func emptyReason(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

// segmentCouldMatch 依据段元数据判断该段是否可能命中请求。
func segmentCouldMatch(seg storage.SegmentMeta, rng Range) bool {
	if rng.FromSeq > 0 && seg.MaxSeq < rng.FromSeq {
		return false
	}
	if rng.ToSeq > 0 && seg.MinSeq > rng.ToSeq {
		return false
	}
	// 段 StoredAt 是落盘时间（秒），事件时间与之不同源，仅作粗剪枝：
	// 段完全早于时间下界（含 1 秒粒度余量）时才排除；存疑保留，交给逐条判定。
	if rng.From != nil {
		fromU := rng.From.Unix()
		if seg.LastStored > 0 && seg.LastStored < fromU-1 && seg.Sealed {
			// 最后一条落盘都早于下界：段内事件时间不可能晚于它（落盘不早于事件产生）。
			return false
		}
	}
	if rng.To != nil {
		toU := rng.To.Unix()
		if seg.FirstStored > 0 && seg.FirstStored > toU+1 && seg.Sealed {
			return false
		}
	}
	return true
}

// buildDeletedIntervals 收集快照中 deleted 段的序号闭区间。
func buildDeletedIntervals(snap storage.ReplaySnapshot, source string) []Gap {
	var out []Gap
	for _, seg := range snap.Segments {
		if seg.Deleted && seg.MaxSeq > 0 {
			out = append(out, Gap{
				FromSeq: seg.MinSeq, ToSeq: seg.MaxSeq, Kind: string(SkipPurged),
				Reason: fmt.Sprintf("segment %s was purged by retention", seg.Name),
			})
		}
	}
	return out
}

// classifyGap 判定 [lo,hi] 空洞：若与已清理段区间相交则记 purged，否则 missing。
func classifyGap(lo, hi int64, deleted []Gap) Gap {
	for _, d := range deleted {
		if lo <= d.ToSeq && d.FromSeq <= hi {
			glo := lo
			if d.FromSeq > glo {
				glo = d.FromSeq
			}
			ghi := hi
			if d.ToSeq < ghi {
				ghi = d.ToSeq
			}
			return Gap{FromSeq: glo, ToSeq: ghi, Kind: string(SkipPurged),
				Reason: d.Reason + fmt.Sprintf("; missing seq [%d,%d]", glo, ghi)}
		}
	}
	return Gap{FromSeq: lo, ToSeq: hi, Kind: "missing",
		Reason: fmt.Sprintf("seq [%d,%d] never existed in retained storage", lo, hi)}
}
