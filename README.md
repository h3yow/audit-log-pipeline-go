# 本地审计事件采集与合规留存管道（Go）

一个零外部依赖（仅 Go 标准库）的本地审计管道，用本地磁盘 + 内存下游模拟多事件来源、
多下游投递，重点保证：**结论可区分、顺序稳定、保留/冻结可执行、防篡改、
故障与重启后不丢不重、检查点回退被明确拒绝**。

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
<dir>/checkpoints/<hex(source)>.cp      # 每来源检查点状态（带链 MAC）
<dir>/checkpoints/anchors.json          # 全局最新版本锚点（带 MAC）
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

## 6. 日志可解释性

管道使用 `log/slog` 结构化日志，关键节点均输出，单测中以 `t.Logf` 打印核心状态：

- `event_input`：事件 ID/来源/序号/类型/动作/时间戳；
- `policy evaluated`：命中规则、动作、路由、原因；
- `decision`：`accepted/duplicate/malformed/rejected`；
- `delivered`：source/seq/event_id/新检查点水位；
- `delivery failed`：失败原因与“事件保持 durable pending”的说明；
- `checkpoint advance rejected`：回退被拒绝的明确记录；
- `purge complete / FROZEN purge`：清理段数、字节数、冻结跳过数；
- `recovery: source loaded`：重启后已知事件数、水位、待恢复数。

## 7. 本地验证

要求 Go 1.25+（开发环境为 Go 1.26）。

```bash
# 全部单元测试（含并发与数据竞争检测）
go test ./... -race -count=1

# 只跑关键场景
go test ./pkg/pipeline/ -run 'TestDuplicate|TestDownstream|TestRestart|TestConcurrent|TestFreeze' -v
go test ./pkg/storage/ -v   # 追加/清理/冻结/篡改检测
go test ./pkg/checkpoint/ -v # 水位单调/回退拒绝/换复件/锚点篡改

# 端到端演示：重复/非法/拒绝 + 下游故障 + 冻结清理 + 指标
go run ./cmd/auditd -dir ./data/demo
# 再次运行可观察“重启恢复”：已确认事件不重放，pending 事件继续投递
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

## 8. 设计边界（明确取舍）

- 下游为内存模拟，演示“投递侧幂等”契约；接入真实下游时，应由下游提供
  基于 `(source, seq)` 的幂等，或在适配器层维护去重表。
- 帧链按“每分段独立起链”设计，分段连续性由清单链与每段序号区间共同约束；
- 合法清理保留 `deleted` 墓碑条目，因此“清单”本身不被清理，长期运行时
  可通过压缩已删除墓碑（同样需要重算链）扩展，本实现未包含该压缩器。
