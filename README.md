# 本地审计事件采集与合规留存管道（Go）

一个零外部依赖（仅 Go 标准库）的本地审计管道，用本地磁盘 + 内存下游模拟多事件来源、
多下游投递，重点保证：**结论可区分、顺序稳定、保留/冻结可执行、防篡改、
故障与重启后不丢不重、检查点回退被明确拒绝、历史回放可续传**。

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
```

模块划分：

| 包 | 职责 |
|---|---|
| `pkg/event` | 事件结构、严格 JSON 解析/校验、处理结论 `Decision` |
| `pkg/router` | 有序策略规则（deny / accept+route），未命中走默认路由 |
| `pkg/storage` | 分段落盘、帧级 HMAC 哈希链、清单哈希链、保留期/冻结清理、防篡改 |
| `pkg/checkpoint` | 每来源投递水位，单调推进 + 状态链 + 全局锚点文件 |
| `pkg/sink` | 下游接口、内存模拟下游（故障注入、幂等）、按路由分发 |
| `pkg/replay` | 历史回放：范围解析、流式读取、独立 MAC 状态链进度、续传/取消 |
| `pkg/pipeline` | 接入/去重/路由/落盘/投递/检查点编排，启动恢复与指标 |
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
<dir>/checkpoints/<hex(source)>.cp      # 每来源实时投递检查点（带链 MAC）
<dir>/checkpoints/anchors.json          # 实时投递全局版本锚点（带 MAC）
<dir>/replays/<hex(jobID)>.rp           # 每回放任务独立进度（带链 MAC）
<dir>/replays/anchors.json              # 回放全局版本锚点（带 MAC）
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

## 5. 历史回放（historical replay）

合规抽查或下游重建时，需要把过去某来源、某段序号/时间范围内的留存事件，按当初
接入的顺序重新投递到一个**独立的目标下游**。该能力由 `pkg/replay` 提供，与实时
投递共用留存数据但拥有完全独立的进度与目标。

### 5.1 请求与范围语义

`Pipeline.StartReplay(ReplayRequest{ID, Source, FromSeq, ToSeq, FromTime, ToTime})`：

- **序号区间为闭区间，边界均包含**。`FromSeq<=0` 解析为 1；`ToSeq<=0` 解析为
  **任务启动那一刻**该来源的持久化最大序号——回放终点在任务启动时即固定，之后
  新接入的事件不属于本次回放（不会被读取，也不会被误投）。
- **时间区间为闭区间**，按事件自身时间戳过滤；零值表示该侧开放。序号区间与时间
  区间同时给出时取交集；命中序号范围但落在时间窗口外的事件计入
  `skipped.before_time / skipped.after_time`。
- 命中 `deny` 策略、以 `rejected` 状态留存的事件**不重新投递**，计入
  `skipped.rejected`；回放只重放 accepted（当初可投递）的事件。
- 请求区间若与已清理分段相交，任务**立即落盘 `failed` 终态**，错误信息列出缺口的
  序号区间（`replay.ErrDataGap` / 存储层 `storage.ErrPurgedGap`），绝不静默跳过。
- 同一来源同时只允许一个非终态回放（`replay.ErrConflict`）；同一任务 ID 重复提交
  返回 `replay.ErrJobExists`；目标路由未配置 fail-closed 拒绝。

### 5.2 顺序、内存与投递语义

- 读取器（`storage.OpenReplay`）按分段、按帧**严格 seq 递增**地流式产出，逐帧校验
  HMAC 哈希链；每次只在内存中持有一条事件，大批量回放不会把整个范围读入内存。
- 每个分段以打开时快照的 `Size` 为读取上限（`io.LimitReader`）：活动分段的 `Size`
  只在一帧完整写入并 `fsync` 后才在锁内推进，因此即使回放与实时接入并发，也读不到
  “写了一半”的帧。
- 投递语义与实时一致：**at-least-once 重试 + 下游按 `(route, source, seq)` 幂等 =
  effectively-once 可见**。回放走独立路由名（如 `replay`），与实时下游天然隔离；
  下游故障时在**同一序号**上指数退避（50ms→2s）重试，不越过、不乱序，重试次数
  计入 `status.retries`。

### 5.3 独立的持久化进度（与实时检查点互不影响）

```
<dir>/replays/<hex(jobID)>.rp     # 每任务回放进度（带状态链 MAC）
<dir>/replays/anchors.json        # 回放全局版本锚点（带 MAC）
```

- 进度文件结构与实时检查点同构（状态链 + 全局锚点 + 崩溃窗口补提交），但使用
  **独立目录、独立创世锚（`REPLAY-GENESIS:`）与独立 MAC 域前缀
  （`replay-state-v1` / `replay-anchors-v1`）**：两套进度互不读写、互不回带。
- 每处理完一条（投递成功**或**显式分类跳过）立即 fsync 前进的 `Cursor`；下游故障或
  进程重启后从 `Cursor+1` 续跑，不重复、不漏。
- **进度倒退、游标/计数被改小、旧版本文件被换回复刻、MAC 不符**一律
  `replay.ErrRollback`，管理器拒绝打开，绝不从头静默重放。
- `Pipeline.Close()`（进程停机）**不**把任务置为 canceled——未完成任务保持
  `running` 与最新进度，下次启动自动续跑；显式 `CancelReplay` 才落盘
  `canceled` 终态（已投递不回退，重启后不再自动恢复）。

### 5.4 与保留清理、合规冻结的关系

- 启动回放时在存储层**同一把写锁内原子完成**“注册保留锁 + 缺口检查”，Purge 无法
  插入两步之间；之后落在回放区间 `[FromSeq,ToSeq]` 内的分段即使过了保留期也被 Purge
  跳过，计入 `PurgeResult.SkippedReplay`。
- `Purge` 全程持存储写锁，回放读取器在 `RLock` 内深拷贝不可变分段快照后才在锁外读
  文件：读不到清到一半的分段，也不会读到删除后的内容（快照中的文件在锁释放前不会被
  删；若清单与磁盘不一致则报 `storage.ErrTampered`）。
- 冻结与回放保留锁是**正交的两层保护**：冻结面向合规保留（可独立配置/到期），回放锁
  面向“本次任务运行期”；两者任一命中都会阻止清理。回放完成/失败/取消后保留锁释放，
  分段才重新受保留期与冻结规则约束。
- 运行中若保留数据因任何原因缺失（读到删除标记/缺口），任务 `failed` 并给出序号缺口，
  不会继续暴露不完整范围。

### 5.5 进度可观测

`Pipeline.ReplayStatus(id)` / `ListReplays()` 返回：

| 字段 | 含义 |
|---|---|
| `phase` | `running / completed / failed / canceled` |
| `cursor / to_seq` | 已处理到的序号 / 本次确定的终点；`to_seq-cursor` 即剩余 |
| `delivered` | 已重新投递条数 |
| `retries` | 下游失败后的重试次数（原地重试同一序号） |
| `skipped.before_time/after_time` | 在序号范围内但时间窗口外的条数 |
| `skipped.rejected` | 命中 deny、留存但不投递的条数 |
| `skipped.missing` / `last_error` | 命中已清理缺口的分段数与失败原因（failed 时显式解释） |

`pipeline.ReplayRemaining(status)` 给出剩余待处理序号数。

## 6. 故障与重启行为

| 场景 | 行为 |
|---|---|
| 下游持续不可用 | worker 指数退避（50ms→2s 上限）重试当前最小未确认事件；事件留在磁盘 |
| 内存有界 | 每来源在飞内存事件受 `MaxInFlight` 限制，超限停泵，不额外占用内存 |
| 磁盘有界 | `MaxPendingDisk` 软上限，超过后 `Ingest` 返回 `ErrBackpressure`（可重试，不丢） |
| 可观测 | `Pipeline.Metrics()`：in_flight、pending_on_disk、retries、accepted/duplicates/malformed/rejected/delivered、磁盘字节/分段数 |
| 进程重启 | 重建去重索引（扫描磁盘）→ 校验存储与检查点完整性 → 从 `watermark+1` 重放 pending |
| 投递成功后崩溃 | 下游幂等键保证重试不产生重复可见记录 |
| 检查点回退/复刻/锚点篡改 | `ErrRollback`，拒绝启动并记录错误日志 |

## 7. 日志可解释性

管道使用 `log/slog` 结构化日志，关键节点均输出，单测中以 `t.Logf` 打印核心状态：

- `event_input`：事件 ID/来源/序号/类型/动作/时间戳；
- `policy evaluated`：命中规则、动作、路由、原因；
- `decision`：`accepted/duplicate/malformed/rejected`；
- `delivered`：source/seq/event_id/新检查点水位；
- `delivery failed`：失败原因与“事件保持 durable pending”的说明；
- `checkpoint advance rejected`：回退被拒绝的明确记录；
- `purge complete / FROZEN purge`：清理段数、字节数、冻结/回放跳过数；
- `recovery: source loaded`：重启后已知事件数、水位、待恢复数；
- `replay started/resumed/delivered/completed/failed/canceled`：
  回放范围解析、当前序号、已投递/重试数与失败原因。

## 8. 本地验证

要求 Go 1.25+（开发环境为 Go 1.26）。

```bash
# 全部单元测试（含并发与数据竞争检测）
go test ./... -race -count=1

# 只跑关键场景
go test ./pkg/pipeline/ -run 'TestDuplicate|TestDownstream|TestRestart|TestConcurrent|TestFreeze' -v
go test ./pkg/storage/ -v   # 追加/清理/冻结/篡改检测
go test ./pkg/checkpoint/ -v # 水位单调/回退拒绝/换复件/锚点篡改

# 历史回放场景（顺序/边界、崩溃续传、故障重试、回退拒绝、与清理并发）
go test ./pkg/replay/ -race -v
go test ./pkg/storage/ -run 'TestReplay' -race -v
go test ./pkg/pipeline/ -run 'TestReplay' -race -v

# 端到端演示：重复/非法/拒绝 + 下游故障 + 冻结清理 + 指标 + 历史回放
go run ./cmd/auditd -dir ./data/demo
# 再次运行可观察“重启恢复”：已确认事件不重放，pending 事件继续投递，
# 已完成回放保持 completed，未完成回放自动从 Cursor+1 续跑。
```

测试覆盖的关键场景：

- `TestDuplicateAndRejectAndOrder`：重复投递（同 ID / 同 seq）、拒绝、非法输入、顺序；
- `TestDownstreamFailureNoLossNoDuplicate`：下游故障期间有界、水位不动、恢复后不丢不重；
- `TestRestartResumesPending` / `TestRestartAfterPartialDelivery`：重启续传；
- `TestConcurrentWriteAndPurge`（`-race`）：多来源并发写 + 并发读 + 并发清理；
- `TestFreezeDuringPurge`：冻结期间跳过、解除后清理；
- `TestTamperDetectedOnModify` / `TestTamperDetectedOnOrphanSegment`：
  帧改写、孤立分段；
- `TestRestoreOldCopyRejected` / `TestAnchorTamperRejected`：检查点回退与篡改拒绝。

回放相关（测试日志会打印请求范围、判定结果与进度状态）：

- `TestReplayOrderAndRangeBoundaries`：闭区间起终点、开放上界、时间窗部分命中、
  rejected 分类跳过、实时下游不被污染；
- `TestReplayResumeAcrossCrash` / `TestReplayRestartResumes`：下游故障 +
  “进程崩溃”重启后从断点续跑，不重不漏；
- `TestReplayDownstreamFailureRetriesNoDuplicate`：故障期间原地重试同一序号，
  恢复后无重复可见；
- `TestReplayTamperedProgressRejected` / `TestReplayOldCopyRestoredRejected`：
  游标篡改、旧副本换回均被 `ErrRollback` 拒绝；
- `TestReplayCancelIsTerminal`：显式取消落盘终态、重启不自动恢复；
- `TestReplayRangeHitsPurgedGap`：区间命中已清理分段时 failed 并列出缺口；
- `TestReplayReaderConcurrentAppendAndPurge`（`-race`）/
  `TestReplayConcurrentPurgeAndFreeze`：回放与实时接入、清理、冻结并发，
  持锁分段不被清、顺序稳定、无竞争。

## 9. 兼容范围

- 回放为**纯增量能力**：不配置 `Config.ReplaySinkName` 时行为与之前完全一致，
  旧数据目录可直接打开（回放目录按需创建）。
- 实时投递检查点（`<dir>/checkpoints`）与回放进度（`<dir>/replays`）物理隔离、
  MAC 域隔离，互不影响、互不回带；删除回放目录只丢失回放进度（可重新发起任务），
  不影响实时投递与留存数据。
- 回放只读取已落盘留存记录，不改变任何记录的实时投递状态，也不推进实时水位。
- 已知边界：回放区间以**任务启动时**已持久化的最大序号为开放上界，任务运行期间
  新接入的事件不纳入本次任务（既不会被读取也不会被投递）；回放保留锁为内存态，
  仅在管道进程存活期
  生效（与 Purge 同进程，进程停止即无清理发生）。

## 10. 设计边界（明确取舍）

- 下游为内存模拟，演示“投递侧幂等”契约；接入真实下游时，应由下游提供
  基于 `(source, seq)` 的幂等，或在适配器层维护去重表。
- 帧链按“每分段独立起链”设计，分段连续性由清单链与每段序号区间共同约束；
- 合法清理保留 `deleted` 墓碑条目，因此“清单”本身不被清理，长期运行时
  可通过压缩已删除墓碑（同样需要重算链）扩展，本实现未包含该压缩器。
- 回放默认只重放 accepted 事件；rejected 留存事件计入 `skipped.rejected` 而不投递，
  需要“全量留存重取”时应由调用方在适配层按记录状态另行处理。
