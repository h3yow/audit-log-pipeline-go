# 本地审计事件采集与合规留存流水线

一个纯本地、零外部依赖（仅 Go 标准库）的审计事件管道。它用本地文件存储模拟
**多个事件来源** 与 **多个下游**，并保证在重复投递、保留期到期、下游长期不可用、
进程崩溃重启等情况下结论**一致、可解释、可复现**。

## 目录结构

```
internal/
  event/       事件模型、JSON 解析与语义校验、可区分的处理结论
  policy/      去重判定 + 有序路由规则（ALLOW / DENY）
  store/       只增哈希链分段存储、保留期/合规冻结、清理、篡改检测、隔离区
  checkpoint/  单调检查点（回退显式拒绝并留痕）
  spool/       有界磁盘待发队列（每来源 FIFO、重启重建、压缩）
  downstream/  下游接口 + 可开关故障的幂等内存 Mock
  pipeline/    编排：接入→校验→去重→路由→落盘→入队→投递→对账
cmd/auditpipeline/  端到端演示程序
```

## 数据平面（一次提交发生了什么）

`Pipeline.Submit(raw)` 对**每个**输入都返回一个显式回执 `Receipt`，结论可区分：

| 结论 `code`   | 含义                                                                 | 去向 |
|---------------|----------------------------------------------------------------------|------|
| `ACCEPTED`    | 合法、新颖、命中允许策略，已持久化并安排投递                           | 审计存储 + 待发队列 |
| `MALFORMED`   | JSON 无法解析，或缺字段 / seq 非法等语义错误                          | 隔离区 |
| `DUPLICATE`   | 同一来源内相同事件 ID 已被接收（ID 是幂等键，seq 不同也算重复）        | 隔离区 |
| `REJECTED`    | 命中一条 DENY 规则（回执带规则名 `rule` 与原因 `reason`）             | 隔离区 |
| `OVERLOADED`  | 下游故障期间有界磁盘队列已满；事件已落盘，调用方可安全重试同一输入     | 审计存储（投递义务后补） |

**格式非法、重复、被拒绝的输入永不静默丢弃**：它们进入只增、带哈希链的隔离区
（`store/quarantine.log`），可通过 `Pipeline.Quarantine()` 审计回放。

顺序保证：审计存储对追加操作串行化并分配全局自增记录 ID；**同一来源的事件按其
提交顺序稳定落盘与投递**（跨来源不保证全局顺序）。投递队列对每个来源实行“同一时刻
只有一个在途”的严格 FIFO，配合跨来源轮转保证公平。

## 投递语义（故障期）

- **至少一次投递（at-least-once）+ 下游幂等 = 恰好一次可见**。投递载荷携带
  `record_id` / `event_id` 幂等键；`downstream.MockTarget` 对相同记录只产生一次
  可见效果，重试不会产生重复可见事件。
- **故障期间有界且可观测**。待发事件写入磁盘 spool，容量由 `MaxSpoolDiskBytes`
  限定；容量耗尽时返回可区分的 `OVERLOADED`，而不是阻塞或丢数据。`Stats().Spool`
  暴露 `queued / bytes_on_disk / bytes_limit / leased / total_*` 等指标。投递失败采用
  上限指数退避（20ms→2s），不会忙等。
- **崩溃安全**。每条审计记录与每条 spool 帧都 `write + fsync`（并 fsync 目录）。
  进程崩溃后重开同一目录：
  - spool 从日志重建待发集合，尾部半个帧（crash 撕裂）按有效前缀截断；
  - 去重索引从审计存储重建；检查点日志重放恢复每个来源的位置；
  - **对账器（reconciler）** 周期性扫描“已落盘但缺少投递义务”的记录（例如落盘后、
    入队前崩溃，或故障期 `OVERLOADED` 未入队的部分），在下游恢复、spool 有空位时
    自动补齐。它依据每来源的持久“对账水位”只扫描新记录，因此**不会**把已清理的历史
    重新投递。

### 检查点单调，拒绝回退

`checkpoint` 是只增日志。位置只能前进：

- 前进到更大 seq：正常记录并 fsync；
- 相同 seq：幂等空操作（重启重试同一位置安全）；
- 更小 seq：返回 `checkpoint.ErrRegression`，**位置不变**，并追加一条
  `regression` 留痕（记录 from_seq / wanted_seq / 时间），可通过
  `Pipeline.Regressions()` 审计。旧进程拿着陈旧 offset 重启不会触发静默重放。

## 保留期与合规冻结

- 每条记录追加时按来源策略计算 `retain_until = stored_at + retention(source)`。
- 冻结 `PutFreeze(key, fromID, toID)` 是持久化的法律保留（journal），覆盖区间内
  的记录即使过了保留期也不得删除；可随时 `ReleaseFreeze(key)`。
- 清理（`store.Cleanup`）以**已封口段**为单位，且删除集合必须是一个**前缀**，因此：
  - 冻结中或仍在保留期内的记录所在段一律保留；
  - 删除从不制造空洞、不暴露半成品；
  - 删除采用两阶段崩溃安全协议：**先在 manifest 中原子标记删除并 fsync，再删索引项，
    最后 unlink 文件并 fsync 目录**。并发读持读锁解析 ID，已删除记录返回
    `ErrNotFound`，永远读不出已清理数据。

### 篡改检测

每个段内的记录构成独立的 SHA-256 哈希链（帧内含 `prev_hash`、所属段名、段内稠密
序号 `seg_index`）。启动与 `Verify()` 时逐帧重算并校验，可区分：

- `MODIFIED`：记录内容/哈希不符（被改写）；
- `DELETED`：manifest 中存在的活动段文件丢失；
- `REORDERED`：ID 不连续、prev-hash 断链或段内乱序插入；
- `TRUNCATED`：已封口段尾部出现半个帧；
- `INSERTED`：磁盘出现 manifest 未知的段文件（越权插入）；
- `RESURRECTED`：manifest 标记已删的段文件又出现（删除被回滚）。

哈希链**按段独立**，所以正常的前缀清理不会让幸存段“断链”误报。未封口活动段尾部因
崩溃产生的半帧是崩溃产物，会被截断到有效前缀，不计为攻击；已封口段被截断则报警。

## 本地验证

要求 Go 1.25+（仓库在 Go 1.26 下开发测试）。

```bash
# 构建
go build ./...

# 全量单元/集成测试（建议带竞态检测）
go test ./... -race

# 只看关键场景（测试日志会打印事件输入 INPUT、策略判定 DECISION、检查点 CHECKPOINT）
go test ./internal/pipeline/ -run TestDuplicateAndDistinctConclusions -v
go test ./internal/pipeline/ -run TestConcurrentCleanupWhileReading -v
go test ./internal/pipeline/ -run TestFreezeBlocksCleanup -v
go test ./internal/pipeline/ -run TestDownstreamOutageBoundedThenRecover -v
go test ./internal/pipeline/ -run TestCheckpointRegressionRejected -v
go test ./internal/store/      -run TestTamperDetected -v

# 端到端演示（多来源 / 重复非法拒绝 / 故障 / 崩溃重启 / 恢复 / 冻结清理）
go run ./cmd/auditpipeline
```

### 测试覆盖的关键场景

- **重复投递**：相同事件重复提交得到 `DUPLICATE`，下游每个记录仅一次可见；非法、
  重复、拒绝三类输入都在隔离区可审计。
- **并发清理**：多来源并发追加 + 并发读 + 多个清理协程同时执行，无丢失、无重复暴露、
  无半成品，清理后 `Verify()` 零问题。
- **冻结期间清理**：对记录 1..5 施加法律冻结后清理，冻结记录保留且可读；释放冻结后
  才被删除，已删记录 `Get` 返回 `ErrNotFound`。
- **下游故障 + 重启**：下游全程不可用时磁盘占用有界并可观测、超限时给 `OVERLOADED`；
  `HardClose` 模拟崩溃后用**同一目录**重开，待发与检查点恢复；下游恢复后全部事件
  按序、恰好一次可见。
- **检查点回退**：从 seq=10 试图回退到 seq=3 被 `ErrRegression` 拒绝并留痕，位置保持 10。
- **篡改检测**：改写段内字节后重开，`Stats().TamperIssues` 出现 `MODIFIED`；放置未知段
  文件出现 `INSERTED`。

## 设计取舍

- 用“审计存储先落盘 + spool 投递 + 后台对账”解耦接收与投递：接收在下游故障时永远
  成功（直至有界容量），恢复后不丢事件。
- 幂等键下沉到下游，避免分布式事务；至少一次 + 幂等实现“恰好一次可见”。
- 所有一致性结论（非接收、回退、篡改）都落盘留痕，而不是仅在内存中打日志。
