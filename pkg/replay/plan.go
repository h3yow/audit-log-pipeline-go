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

	// 1) 用段元数据推导实际扫描的序号闭区间 [effFrom, effTo]。
	//    时间条件只允许按“段内事件自身时间戳的上下界”剪枝（严格在区间外才排除），
	//    绝不能用落盘时间：迟到数据落盘晚于区间上界，但事件时间仍可能落在区间内。
	//    被整体排除的段先收集到 excluded，最终是否列入 Plan.Pruned 见下方过滤。
	effFrom, effTo := int64(0), int64(0)
	var excluded []PrunedSegment
	for _, seg := range snap.Segments {
		if seg.Deleted || seg.Records == 0 || seg.MaxSeq == 0 {
			continue
		}
		ok, reason := segmentCouldMatch(seg, rng)
		if !ok {
			if reason != "" {
				excluded = append(excluded, PrunedSegment{
					Name: seg.Name, MinSeq: seg.MinSeq, MaxSeq: seg.MaxSeq,
					Kind: SkipOutsideTime, Reason: reason,
				})
			}
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
		// 没有扫描区间：被排除的段不会经过逐条判定，必须全部段级说明。
		plan.Pruned = excluded
		plan.Note = fmt.Sprintf("no retained segment intersects the requested range; pruned=%d", len(excluded))
		return plan, nil
	}
	// 与扫描区间 [effFrom,effTo] 相交的排除段（乱序事件时间夹在不排除段
	// 之间时会出现）：其记录会在扫描阶段被逐条判定并计入 Skipped，
	// 不再段级重复列出；完全在扫描区间之外的排除段必须段级说明，
	// 保证“命中 + 逐条跳过 + 段级排除”合计覆盖请求触及的全部留存。
	for _, p := range excluded {
		if p.MaxSeq < effFrom || p.MinSeq > effTo {
			plan.Pruned = append(plan.Pruned, p)
		}
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
			plan.addSkipped(Skipped{Seq: seq, Kind: SkipBeforeRange,
				Reason: fmt.Sprintf("seq %d < from_seq %d (partially matched segment)", seq, rng.FromSeq)})
			return true
		}
		if rng.ToSeq > 0 && seq > rng.ToSeq {
			plan.addSkipped(Skipped{Seq: seq, Kind: SkipAfterRange,
				Reason: fmt.Sprintf("seq %d > to_seq %d (partially matched segment)", seq, rng.ToSeq)})
			return true
		}
		// 接入时被拒绝：留存但不投递（含回放，语义一致且可解释）。
		if rv.Record.State == storage.StateRejected {
			plan.addSkipped(Skipped{Seq: seq, Kind: SkipRejected,
				Reason: emptyReason(rv.Record.Decision, "event was rejected at ingest")})
			return true
		}
		// 时间边界（事件自身时间戳，两端均为闭区间、端点时刻算命中）。
		ts := rv.Record.Event.Timestamp
		if rng.From != nil && ts.Before(*rng.From) {
			plan.addSkipped(Skipped{Seq: seq, Kind: SkipOutsideTime,
				Reason: fmt.Sprintf("timestamp %s before from %s", ts.Format(time.RFC3339), rng.From.Format(time.RFC3339))})
			return true
		}
		if rng.To != nil && ts.After(*rng.To) {
			plan.addSkipped(Skipped{Seq: seq, Kind: SkipOutsideTime,
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
	plan.Note = fmt.Sprintf("range resolved to seq [%d,%d]; matched=%d gaps=%d skipped=%d pruned=%d",
		effFrom, effTo, plan.Matched, len(plan.Gaps), plan.SkippedTotal, len(plan.Pruned))
	return plan, nil
}

// maxSkippedEntries 限制 Plan.Skipped 逐条说明的条数上限：
// 大批量回放（几百 MB 留存）可能跳过大量记录，逐条说明会让进度文件无界增长；
// 超出部分只计入 SkippedTotal，段级排除由 Pruned 说明。
const maxSkippedEntries = 2048

// addSkipped 记录一条逐条跳过说明；超出上限后只计数。
func (p *Plan) addSkipped(s Skipped) {
	p.SkippedTotal++
	if len(p.Skipped) < maxSkippedEntries {
		p.Skipped = append(p.Skipped, s)
	}
}

func emptyReason(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}

// segmentCouldMatch 依据段元数据判断该段是否可能命中请求。
// 返回 false 时，若 reason 非空表示这是按时间条件做的段级排除，
// 调用方必须把它记入 Plan.Pruned（绝不静默忽略）；reason 为空表示
// 该段仅在序号请求范围之外，本就不属于本次判定对象，无需说明。
//
// 时间剪枝只使用段内“事件自身时间戳”的上下界（MinEventTS/MaxEventTS），
// 绝不使用落盘时间（FirstStored/LastStored）：迟到数据落盘时间远晚于
// 事件时间，按落盘时间剪枝会把本该命中的整段静默丢掉。
// 时间区间两端均为闭区间：段界与端点相等仍可能命中，只有严格落在
// 区间之外才允许排除。界未知（0）或未封存的段一律不排除，
// 交给逐条判定（安全方向）。
func segmentCouldMatch(seg storage.SegmentMeta, rng Range) (bool, string) {
	if rng.FromSeq > 0 && seg.MaxSeq < rng.FromSeq {
		return false, ""
	}
	if rng.ToSeq > 0 && seg.MinSeq > rng.ToSeq {
		return false, ""
	}
	if !seg.Sealed || seg.MinEventTS <= 0 || seg.MaxEventTS <= 0 {
		return true, ""
	}
	if rng.From != nil && seg.MaxEventTS < rng.From.UnixNano() {
		return false, fmt.Sprintf("segment %s event-time upper bound %s is strictly before range from %s",
			seg.Name,
			time.Unix(0, seg.MaxEventTS).UTC().Format(time.RFC3339),
			rng.From.UTC().Format(time.RFC3339))
	}
	if rng.To != nil && seg.MinEventTS > rng.To.UnixNano() {
		return false, fmt.Sprintf("segment %s event-time lower bound %s is strictly after range to %s",
			seg.Name,
			time.Unix(0, seg.MinEventTS).UTC().Format(time.RFC3339),
			rng.To.UTC().Format(time.RFC3339))
	}
	return true, ""
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
