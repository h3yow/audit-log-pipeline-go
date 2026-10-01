# 本地审计事件采集与合规留存管道（Go）

一个零外部依赖（仅 Go 标准库）的本地审计管道，用本地磁盘 + 内存下游模拟多事件来源、
多下游投递，重点保证：**结论可区分、顺序稳定、保留/冻结可执行、防篡改、
故障与重启后不丢不重、检查点回退被明确拒绝**，并支持**按序号/时间范围把历史事件
顺序回放到独立下游（独立持久化进度、中断续传、与保留清理/冻结和平共处）**。

## 1. 架构总览

```
event.ParseEvent ──► Ingest(每来源串行)
                        │
          ┌─────────────┼──────────────────────────┐
          ▼             ▼                          ▼
     格式校验        内存去重索引(按 eventID)   router 策略(deny/accept+route)
          │             │                          │
          ▼             ▼                          ▼
   malformed 结论  duplicate 结论        storage.Append（顺序落盘 + fsync + HMAC 哈希链）
                                                     │
                                   每来源一个 worker（顺序投递）
                                                     │
                                        下游幂等 Deliver(route, source, seq)
                                                     │
                                  checkpoint.Advance（单调水位 + 锚点链）

  历史回放（与实时路径并行、独立）：
  StartReplay(jobID, source, target, 序号/时间闭区间)
        │  清单快照静态判定 Plan(matched/gaps/skipped，逐条可解释)
        ▼
  Store.Scan 按 seq 升序流式扫描 ──► 独立下游 ReplaySinks[target]
        │  每条确认后写 <dir>/replay/<job>.job（独立哈希链，防回退）
        ▼
  Purge 回放占位 SkippedReplay；重启后 ResumeAll 从 next_seq 续跑
```

模块划分：

| 包 | 职责 |
|---|---|
| `pkg/event` | 事件结构、严格 JSON 解析/校验、处理结论 `Decision` |
| `pkg/router` | 有序策略规则（deny / accept+route），未命中走默认路由 |
| `pkg/storage` | 分段落盘、帧级 HMAC 哈希链、清单哈希链、保留期/冻结清理、防篡改 |
| `pkg/checkpoint` | 每来源实时投递水位，单调推进 + 状态链 + 全局锚点文件 |
| `pkg/replay` | 按范围历史回放：范围静态判定、独立持久化进度链、顺序流式投递、保留占位 |
| `pkg/sink` | 下游接口、内存模拟下游（故障注入、幂等）、按路由分发 |
| `pkg/pipeline` | 接入/去重/路由/落盘/投递/检查点/回放编排，启动恢复与指标 |
| `cmd/auditd` | 可本地运行的端到端演示 |

## 2. 投递语义（delivery semantics）

系统提供**面向下游可见性的 effectively-once**，由两层共同保证：

1. **存储与重放层 = at-least-once**
   - 事件先 `Append` 落盘（每帧写后 `fsync`），落盘成功后才进入投递；
   - 仅当下游 `Deliver` 成功且检查点水位推进后，事件才算完成；
   - 下游故障或进程崩溃时，事件保持 `pending`，重启/恢复后会被**重复投递尝试**，
     此时检查点**不会**推进，故障期间水位保持不动（可观测）。
2. **下游去重 = 幂等键 `(route, source, seq)`**
   - `sink.MemorySink` 以 `(route, source, seq)` 为主键保存可见事件；
   - 同一键的重复投递返回成功但不产生第二条可见记录。
   - 因此崩溃发生在“投递成功、检查点未写”窗口时，重试不会产生重复可见事件。

崩溃窗口的其它两种情况也有明确定义：

- **状态文件已写、锚点未更新**：重启时识别为合法的前向提交，自动补提锚点；
- **旧版本检查点文件被换回复刻 / 水位倒退 / MAC 被改**：`checkpoint.Open`
  直接返回 `checkpoint.ErrRollback`，管道拒绝启动，**绝不静默重放**。

顺序保证：每个来源有独立的接入互斥锁（`Append` 强制 seq 单调递增）与独立的
投递 worker（从 `watermark+1` 开始顺序投递，不越过空洞），因此**同一来源内
事件的接入顺序与可见顺序稳定**；不同来源之间互不阻塞。

## 3. 去重规则

去重在接入路径上产生显式结论，任何事件都不会被静默丢弃：

| 输入情况 | 结论 `DecisionKind` | 说明 |
|---|---|---|
| 空体/非法 JSON/非法时间戳/未知字段 | `malformed` | `IngestRaw` 捕获，含失败原因 |
| 必填字段缺失、seq 非正 | `malformed` | `Ingest` 中校验 |
| 相同 `eventID` 再次出现 | `duplicate` | 内存索引（启动时由磁盘全量重建） |
| 相同来源相同 `seq`、不同 ID | `duplicate` | 存储层 `SequenceError` 显式映射 |
| 命中 deny 规则 | `rejected` | **仍落盘留存**（状态 `rejected`），只是不投递，附规则名与原因 |
| 命中/兜底 accept | `accepted` | 落盘为 `pending`，路由到目标下游 |

路由规则按声明顺序匹配，先命中先生效；`accept` 规则未配置 route 被视为
配置错误并**拒绝**（fail-closed），不会静默放行。

## 4. 存储、保留与合规冻结

### 4.1 磁盘布局

```
<dir>/store/key.bin                     # HMAC-SHA256 密钥（32B，0600）
<dir>/store/freezes.json                # 冻结集合（带 MAC，防篡改）
<dir>/store/src/<hex(source)>/manifest.json
<dir>/store/src/<hex(source)>/seg-000001.dat ...
<dir>/checkpoints/<hex(source)>.cp      # 每来源实时投递检查点状态（带链 MAC）
<dir>/checkpoints/anchors.json          # 实时检查点全局最新版本锚点（带 MAC）
<dir>/replay/<hex(jobID)>.job           # 每个回放任务的独立进度（带链 MAC）
<dir>/replay/anchors.json               # 回放进度全局最新版本锚点（带 MAC）
```

### 4.2 防篡改（双层哈希链）

- **帧链**：分段内每条记录为 `[magic][version][len][body][HMAC]`，
  `HMAC_i = HMAC(key, MAC_{i-1} ‖ body_i)`，首帧前驱为全零。
  改写任意字节 → 该帧或后续帧 MAC 校验失败；删除中间帧/乱序插入 →
  帧链断裂或序号不连续。
- **清单链**：`manifest.json` 中每个分段条目含
  `HMAC(前驱条目摘要 ‖ 条目全部字段)`，并记录序号区间、记录数、首末时间、
  封存标记等。文件被改写、条目被删除、伪造孤立分段、分段名乱序插入都会在
  `Open`/`Verify` 时返回 `storage.ErrTampered`。
- 进程崩溃留下的**写一半的尾帧**（torn frame）与恶意篡改可区分：
  前者是文件末尾的不完整帧，启动时安全截断修复；MAC/魔数错误一律
  `ErrTampered`，不做自动修复。

### 4.3 保留期清理（Purge）

- 仅清理**已封存（sealed）且非活动**分段；活动分段永不删除；
- 分段末条记录时间早于 `now - Retention.MaxAge` 才进入候选；
- 删除文件后，清单条目被标记为 `deleted` 并**保留在哈希链上**（删除动作可审计），
  随后原子重写清单；已清理分段对 `Get/Scan` 不再可见（读后返回 `ErrNotFound`）；
- `Purge` 全程持存储写锁，与接入写、扫描读互斥，杜绝丢失、重复暴露或半成品状态；
  `Scan` 在锁内深拷贝不可变快照后在锁外读文件，支持并发读写。

### 4.4 合规冻结（Freeze）

- 冻结以 `(source 可空=全部, fromSeq, toSeq 可开放上界, until)` 表示；
- 清理时与任一**仍生效**冻结范围相交的分段一律跳过并计入 `SkippedFrozen`；
- 冻结清单 `freezes.json` 带 HMAC，篡改冻结以绕过保留期会被 `Open` 检测；
- 显式 `RemoveFreeze` 或冻结到期后，同分段才重新可被清理。

## 5. 故障与重启行为

| 场景 | 行为 |
|---|---|
| 下游持续不可用 | worker 指数退避（50ms→2s 上限）重试当前最小未确认事件；事件留在磁盘 |
| 内存有界 | 每来源在飞内存事件受 `MaxInFlight` 限制，超限停泵，不额外占用内存 |
| 磁盘有界 | `MaxPendingDisk` 软上限，超过后 `Ingest` 返回 `ErrBackpressure`（可重试，不丢） |
| 可观测 | `Pipeline.Metrics()`：in_flight、pending_on_disk、retries、accepted/duplicates/malformed/rejected/delivered、磁盘字节/分段数 |
| 进程重启 | 重建去重索引（扫描磁盘）→ 校验存储与检查点完整性 → 从 `watermark+1` 重放 pending |
| 投递成功后崩溃 | 下游幂等键保证重试不产生重复可见记录 |
| 检查点回退/复刻/锚点篡改 | `ErrRollback`，拒绝启动并记录错误日志 |

## 5A. 历史回放（replay）

合规抽查或下游重建时，可以把**过去某个来源、某段序号或某段时间**内留存的
事件，按**当初接入的顺序**重新投递到一个**独立的目标下游**。回放与实时投递
并行运行、互不影响。

### 5A.1 范围与判定（边界必须可解释）

`Pipeline.StartReplay(jobID, source, target, ReplayRange{FromSeq,ToSeq,From,To})`

- 区间全部是**闭区间**；序号与时间可以单独给，也可以组合（同时满足才命中）；
  至少要给一个边界，否则返回 `replay.ErrInvalidRequest`；
- **时间命中按事件自身时间戳 `event.timestamp` 判定，与落盘时间 `StoredAt` 无关。**
  这一点对“很早发生、过了很久才接入落盘”的历史批次至关重要：它们的
  `StoredAt` 整体晚于回放时间上界，但只要事件时间落在区间内就必须命中。
  因此清单层面的**段级剪枝只使用序号区间**（`MinSeq/MaxSeq` 不相交才排除），
  **绝不使用 `StoredAt` 推断事件时间**；凡时间条件一律落到逐条判定
  （`from <= event.timestamp <= to`，两端时刻本身命中）。这样大批量历史
  数据（多个已封存段、落盘时间很晚）也不会被整段默默剪掉；
- 开始前在清单快照上做一次**静态判定（Plan）**，结果持久化进任务：
  - `eff_from_seq/eff_to_seq`：实际扫描的序号闭区间（仅依据分段序号区间剪枝）；
  - `matched`：通过全部过滤、应当投递的记录总数；
  - `gaps`：请求序号覆盖范围内的缺失序号段，区分为 `purged`（命中 `deleted`
    清单项，即被保留期清理）与 `missing`（该序号从未存在于留存），都附判定依据；
  - `skipped`：逐条记录的跳过原因，**没有任何一条被默默跳过**：
    `before_range/after_range`（部分命中的分段）、`outside_time`（在序号范围内
    但不在时间区间内，含“区间整体早/晚于本批事件”的情形）、
    `rejected`（接入时命中拒绝策略，留存但回放也不投递）；
  - 对纯时间区间回放，`matched + len(skipped)` 等于扫描范围内的留存记录总数，
    缺失序号另在 `gaps` 中列出——三者可交叉对账，不会出现“没投出去也没解释”；
- 计划是**快照语义**：扫描区间在发起时确定，回放进行中新接入、且落在范围内的
  事件不属于本次任务（进度 `next_seq` 越过计划右端即完成），需要重放可另开任务；
- 同一 `jobID` 是幂等任务键：重复发起返回 `replay.ErrExists`，绝不重放第二遍。

### 5A.2 顺序、内存与幂等

- 顺序：复用 `Store.Scan` 按 seq 升序流式扫描，**回放可见顺序 == 原始接入顺序**；
- 内存有界：逐条扫描、逐条投递，任何时刻内存中只有当前一条，
  不会把整个范围一次性读进内存；空洞（purged/missing）被顺序穿过但不投递；
- 幂等：投递成功后才持久化进度；下游失败或进程崩溃会重复尝试同一条，
  目标下游必须按 `(source, seq)` 幂等（与实时投递同一契约），
  因此**重试与重启续传都不产生重复可见事件**；
- 目标下游是**独立配置**（`Config.ReplaySinks` / `RegisterReplaySink`），
  与实时 `Sinks` 物理隔离，路由名互不影响。

### 5A.3 独立且防回退的持久化进度

```
<dir>/replay/anchors.json            全部回放任务的最新版本锚点（带 MAC）
<dir>/replay/<hex(jobID)>.job        单任务进度（带链 MAC）
```

- 与实时 `<dir>/checkpoints/` **物理隔离、域前缀独立**：
  回放有自己的水位（`next_seq`），不读写实时检查点；
  **回放进度推进不会回带实时水位，实时投递也不影响回放位置**；
- 进度每确认一条就原子落盘（先写状态、再提交锚点），字段包括
  `next_seq / delivered / retries / status / version`；
- 与实时检查点同样的哈希链 + 锚点规则：版本严格 +1、`PrevMAC` 必须接上当前锚点；
  崩溃在“状态已写、锚点未提”窗口可自动补提；**旧副本换回 / 版本倒退 / MAC 被改
  一律 `replay.ErrRollback`，拒绝继续，绝不静默从头重来一遍**；
- 进度可随时查询：`ReplayProgress(jobID)` / `AllReplays()` 给出当前位置、
  已投数、剩余数（`matched - delivered`）、失败重试次数与状态
  （`running/blocked/complete`）。

### 5A.4 与保留清理、合规冻结的关系

- **保留占位（replay hold）**：任务启动即在存储层登记 `[eff_from,eff_to]` 占位。
  `Purge` 删除分段前除冻结检查外，还会检查回放占位，相交的已封存到期分段
  **跳过并计入 `PurgeResult.SkippedReplay`**；活动分段本就永不清理；
- 占位在**发起判定之前**就先按请求最大可能区间登记，判定完成后收窄，
  因此不存在“先判定、后被清掉”的窗口；回放**完成后释放**占位，清理随即恢复；
- 存储的写锁与 `Scan` 的不可变快照保证回放不会读到清到一半的分段，
  也不会读到 `deleted` 之后的内容；
- 冻结与回放上互不干扰：被冻结的数据对回放正常可见；冻结阻止的是清理，
  回放占位同样阻止清理，两者叠加时清理只会更保守；
- 进程重启后内存占位丢失：`Pipeline.New` → `ResumeAll` 会为每个未完成任务
  **重新登记占位**；若目标下游尚未配置，任务进入 `blocked`（占位仍登记），
  `RegisterReplaySink` 后 `ResumeReplay(jobID)` 即可继续。

### 5A.5 故障与重启行为（回放）

| 场景 | 行为 |
|---|---|
| 回放下游故障 | 当前条保持为 `next_seq`，指数退避重试，`retries` 累加；不越过、不丢 |
| 进程重启（中途） | 重新校验进度链 → 重新登记保留占位 → 从持久化 `next_seq` 续跑 |
| 重启时下游未配置 | 任务 `blocked`，注册下游后 `ResumeReplay`；已完成任务不重放 |
| 进度文件倒退/换旧副本/篡改 | `replay.ErrRollback`，拒绝打开 |
| 请求范围含已清理空洞 | `Plan.Gaps` 显式标注 `purged` 及依据，其余正常投递 |
| 请求范围无任何留存 | `Plan.Matched=0` 并给出说明，任务正常完成 |
| 大批量回放 | 流式扫描 + 逐条投递，内存常驻与范围大小无关 |

## 6. 日志可解释性

管道使用 `log/slog` 结构化日志，关键节点均输出，单测中以 `t.Logf` 打印核心状态：

- `event_input`：事件 ID/来源/序号/类型/动作/时间戳；
- `policy evaluated`：命中规则、动作、路由、原因；
- `decision`：`accepted/duplicate/malformed/rejected`；
- `delivered`：source/seq/event_id/新检查点水位；
- `delivery failed`：失败原因与“事件保持 durable pending”的说明；
- `checkpoint advance rejected`：回退被拒绝的明确记录；
- `purge complete / FROZEN purge`：清理段数、字节数、冻结跳过数、回放占位跳过数；
- `recovery: source loaded`：重启后已知事件数、水位、待恢复数；
- `replay started / complete`、`replay delivery failed`：回放范围判定
  （eff 区间、matched/gaps/skipped）、当前位置、重试与完成状态。

## 7. 本地验证

要求 Go 1.25+（开发环境为 Go 1.26）。

```bash
# 全部单元测试（含并发与数据竞争检测）
go test ./... -race -count=1

# 只跑关键场景
go test ./pkg/pipeline/ -run 'TestDuplicate|TestDownstream|TestRestart|TestConcurrent|TestFreeze' -v
go test ./pkg/pipeline/ -run 'TestReplay' -v   # 回放：顺序/边界/续传/清理并发/故障重试
go test ./pkg/replay/ -v                       # 回放进度链、回退拒绝、中途崩溃续传
go test ./pkg/storage/ -v   # 追加/清理/冻结/回放占位/篡改检测
go test ./pkg/checkpoint/ -v # 水位单调/回退拒绝/换复件/锚点篡改

# 时间区间回放专项（按事件时间命中、晚落盘历史批次、边界/跨留存/重启/并发）
go test ./pkg/pipeline/ -run 'TestReplayTimeRange' -v
go test ./pkg/replay/ -run 'TestSegmentCouldMatchIgnoresStoredAt|TestPlanLateStoredBatchReconciles' -v

# 端到端演示：重复/非法/拒绝 + 下游故障 + 冻结清理 + 序号回放
#   + 晚落盘历史批次的时间区间回放（hist-4..hist-8 命中）+ 指标
go run ./cmd/auditd -dir ./data/demo
# 再次运行可观察“重启恢复”：已完成回放不重放（ErrExists），pending 事件继续投递
```

### 7.1 本地复现“时间区间回放命中晚落盘历史数据”

问题场景：事件自身时间很早，但成批很晚才接入落盘（`StoredAt` 晚于回放区间
上界）。修复前会因按落盘时间剪枝而 `matched=0`、投递 0 条却报完成；修复后
按事件自身时间命中。最小复现路径：

1. 接入一批事件，`timestamp` 设为历史时刻（如 `2026-01-01T00:00:0xZ`），
   在“现在”调用 `Ingest`（落盘时间即当下）；调小分段阈值
   （`store.SetMaxSegmentBytesForTest`）并 `SealActiveSegment`，
   让数据分布在多个**已封存**段上（模拟几百 MB 历史留存）。
2. 发起纯时间区间回放：`StartReplay(job, source, target,
   ReplayRange{From: t1, To: t2})`，`[t1,t2]` 落在历史事件时间内、
   但整体早于当下的落盘时间。
3. 断言：`Plan.Matched` 与区间内事件数一致；目标下游按 seq 升序收到且仅收到
   这些事件；`Plan.Skipped` 对其余记录逐条给出 `outside_time`；
   `Delivered == Matched`、`Remaining()==0`、状态 `complete`。
4. 变体：`-race` 下并发 `Purge/AddFreeze/Ingest`（回放占位使清理跳过相关段，
   不读到已删内容）；投递确认一部分后关闭重开，从持久化 `next_seq` 续跑，
   最终集合/顺序不变。

对应自动化用例：

- `TestReplayTimeRangeLateStoredBatch`：600 条晚落盘历史事件跨多个已封存段，
  时间闭区间命中 101 条，断言投递集合、顺序、`matched+skipped==600`；
- `TestReplayTimeRangeBoundariesInclusive`：两端时刻本身命中、部分命中逐条说明、
  单侧 `From` 边界；
- `TestReplayTimeRangeSpansRetainedHistory`：区间跨越一个被保留期清理的中段，
  命中投递 + `gaps.purged` 显式缺失，数量可交叉对账；
- `TestReplayTimeRangeLateStoredResumeAfterRestart` /
  `TestReplayTimeRangeLateStoredMidwayCrash`：重启 / 中途崩溃后按
  `next_seq` 续跑，剩余部分不漏不重；
- `TestReplayTimeRangeLateStoredConcurrentPurgeFreeze`（`-race`）：
  大批量时间回放与清理/冻结并发，占位保护、顺序与集合正确；
- `TestSegmentCouldMatchIgnoresStoredAt` /
  `TestPlanLateStoredBatchReconciles`（`pkg/replay`）：段级剪枝只看序号、
  Plan 对账与“区间整体早于本批事件”时的逐条 `outside_time` 说明。

测试覆盖的关键场景：

- `TestDuplicateAndRejectAndOrder`：重复投递（同 ID / 同 seq）、拒绝、非法输入、顺序；
- `TestDownstreamFailureNoLossNoDuplicate`：下游故障期间有界、水位不动、恢复后不丢不重；
- `TestRestartResumesPending` / `TestRestartAfterPartialDelivery`：实时投递重启续传；
- `TestConcurrentWriteAndPurge`（`-race`）：多来源并发写 + 并发读 + 并发清理；
- `TestFreezeDuringPurge`：冻结期间跳过、解除后清理；
- `TestTamperDetectedOnModify` / `TestTamperDetectedOnOrphanSegment`：
  帧改写、孤立分段；
- `TestRestoreOldCopyRejected` / `TestAnchorTamperRejected`：检查点回退与篡改拒绝；
- `TestOrderAndSeqBounds` / `TestTimeRange`：回放序号/时间闭区间边界、
  部分命中分段的逐条跳过解释、接入时 rejected 不回放；
- `TestDownstreamFailureRetriesNoDuplicate`：回放下游故障重试不重复可见；
- `TestMidwayCrashResumeFromDurableProgress`：中途崩溃后按持久化 `next_seq` 续跑；
- `TestProgressPersistsAcrossRestart`：已完成回放重启不重放；
- `TestRollbackRejected`：回放进度篡改/旧副本被拒绝；
- `TestReplayEndToEndRangeAndIndependence`：真实存储上的范围命中、
  独立回放下游、实时水位不受回放影响；
- `TestReplayHoldBlocksPurge`：回放进行中清理跳过占位段，完成后恢复清理；
- `TestReplayConcurrentWithIngestPurgeFreeze`（`-race`）：
  大批量回放与并发接入/清理/冻结互不破坏、顺序不变；
- `TestReplayTimeRangeLateStoredBatch`：**晚落盘历史批次的时间区间回放**
  （多已封存段、事件时间在区间内而 StoredAt 晚上界），命中集合/顺序/对账；
- `TestReplayTimeRangeBoundariesInclusive`：时间闭区间两端时刻本身命中、
  一段数据部分命中逐条 `outside_time`、单侧边界；
- `TestReplayTimeRangeSpansRetainedHistory`：区间跨越被清理段，
  命中投递 + `gaps.purged` 显式缺失，数量互相对账；
- `TestReplayTimeRangeLateStoredResumeAfterRestart` /
  `TestReplayTimeRangeLateStoredMidwayCrash`：时间回放重启 / 中途崩溃后续跑；
- `TestReplayTimeRangeLateStoredConcurrentPurgeFreeze`（`-race`）：
  晚落盘批次时间回放与清理/冻结并发，不漏投、不读已删、顺序不变；
- `TestReplayRetriesNoDuplicateVisible`：回放故障窗口后无重复可见。

## 8. 设计边界（明确取舍）

- 下游为内存模拟，演示“投递侧幂等”契约；接入真实下游时，应由下游提供
  基于 `(source, seq)` 的幂等，或在适配器层维护去重表。**回放目标下游适用
  完全相同的幂等契约**，否则故障重试可能产生重复可见。
- 回放范围判定是**发起时快照**：进行中新增且落在范围内的事件不纳入本次任务，
  需要它们请另开任务；回放默认不投递接入时被 `rejected` 的事件（留存不变）。
- 时间范围过滤**只按事件 `timestamp` 判定**（闭区间，两端时刻本身命中）；
  段级剪枝**只按序号区间**，不使用落盘时间 `StoredAt`——历史事件可能很晚才
  接入，`StoredAt` 与事件时间不同源，用它剪枝会把本该命中的整段默默丢掉。
- 回放占位只在任务生命周期内阻止清理；若事件在**发起回放之前**就已被清理，
  不会报错中止，而是在 `Plan.Gaps` 中以 `purged` 显式列出，其余照常回放。
- 帧链按“每分段独立起链”设计，分段连续性由清单链与每段序号区间共同约束；
- 合法清理保留 `deleted` 墓碑条目，因此“清单”本身不被清理，长期运行时
  可通过压缩已删除墓碑（同样需要重算链）扩展，本实现未包含该压缩器。
