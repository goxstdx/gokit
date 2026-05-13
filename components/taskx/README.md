# taskx — 统一任务队列与定时任务框架

## 概述

`taskx` 是一个面向多机部署的任务调度框架，提供三种能力：

| 能力 | 说明 | 底层结构（Redis） |
|------|------|------------------|
| **EventQueue** | 实时事件队列，生产即消费 | List（LPUSH / BLMOVE） |
| **DelayQueue** | 延迟队列，指定时间触发 | ZSet（ZADD / ZRANGEBYSCORE） |
| **TimerTask** | 定时任务，cron 表达式驱动 | robfig/cron/v3 + 分布式锁 |

## 目录结构

```
components/taskx/
├── core/                    # 共享类型（QueueRunner, Envelope, Logger 等）
│   └── type.go
├── driver/                  # 驱动接口（纯接口，零实现）
│   ├── event_queue.go       #   EventQueueDriver
│   ├── delay_queue.go       #   DelayQueueDriver
│   └── lock.go              #   LockDriver
├── provider/                # 驱动实现（按技术栈分目录）
│   └── redis/               #   Redis/Valkey 驱动
│       ├── event_queue.go
│       ├── delay_queue.go
│       ├── lock.go
│       └── script.go        #   Lua 脚本（HashTag 兼容集群）
├── queue/                   # 队列消费管理器
│   ├── event_queue.go       #   EventConsumer（N 个消费协程）
│   └── delay_queue.go       #   DelayConsumer（1 轮询 + N worker）
├── timer/                   # 定时任务
│   └── timer_task.go        #   Scheduler（cron 封装）
├── type.go                  # 重新导出 core 类型
├── option.go                # WithXxx Option 配置
├── registry.go              # 统一注册中心
├── manager.go               # 生命周期管理（Start / Stop）
├── export.go                # 对外 API + NewRedisManager 快捷构造
└── taskx_test.go            # 单元测试
```

## 设计思路

### 1. 驱动可替换

核心逻辑只依赖 `driver/` 下的接口，不直接依赖 Redis。`provider/redis/` 是默认实现，后续可在 `provider/` 下新增 `memory/`、`kafka/` 等目录，对现有代码零侵入。

### 2. 三队列模型（pending → processing → dead）

每个 Runner 在 Redis 中有三个 key：

```
taskx:event:{runner_name}:pending      # 待消费
taskx:event:{runner_name}:processing   # 执行中
taskx:event:{runner_name}:dead         # 死信
```

- **pending → processing**：消费者取出消息时通过 `BLMOVE`（Redis 6.2+）原子转移，确保消息不丢
- **processing → 删除**：执行成功后 Ack
- **processing → pending**：执行失败但未超重试上限，通过 Lua 脚本原子完成"删旧值 + 入新值"
- **processing → dead**：超过 `MaxRetry` 进入死信队列；`MaxRetry` 表示额外重试次数，总尝试次数为 `1 + MaxRetry`
- **dead → pending**：通过 `RecoverEventDead` / `RecoverDelayDead` 手动恢复，恢复时自动重置重试计数

### 3. 消息信封（Envelope）

原始 payload 被包装在 Envelope 中，附带元数据：

```json
{
  "id": "550e8400-e29b-41d4-a716-446655440000",
  "payload": "业务数据",
  "retry_count": 0,
  "created_at": 1715500000
}
```

`id` 为 UUID，保证 DelayQueue ZSet 中每条消息的 member 唯一，避免相同 payload 被去重。

重试时 `retry_count` 递增，超过 `MaxRetry` 后进入死信。死信恢复时 `retry_count` 重置为 0。

> **损坏消息处理策略**：如果消费阶段无法解析 Envelope，说明消息信封已经损坏，重复投递大概率仍无法修复，并可能形成 poison message 无限循环。框架会触发 `OnAlert(AlertCorruptMessage)` 后 Ack 删除该消息，不再进入重试或死信；调用方应通过告警排查生产端或历史脏数据。死信恢复时遇到损坏 Envelope 也会触发告警并跳过。

### 4. 崩溃恢复

启动时会启一个一次性恢复协程：

```
抢分布式锁 → 等待 processingTimeout → 将 processing 残留消息移回 pending → 释放锁 → 退出
```

- **等待 processingTimeout**：给活跃的消费者足够时间处理完，降低误恢复正在执行消息的概率
- **分布式锁**：多机启动时只有一台执行恢复；恢复期间会自动续租，降低长恢复导致锁过期的并发恢复风险
- 等待结束后，processing 中还残留的消息会被视为可恢复消息并移回 pending；如果业务处理时间超过 `processingTimeout`，可能发生重复投递，业务侧需保持幂等或调大该配置
- 恢复循环设置了最大恢复时长，超限会告警并提前退出，避免极端情况下无限恢复

> **后续优化方向**：EventQueue 可考虑将 processing 改为带时间戳结构；DelayQueue 可考虑处理期间刷新租约/score；也可以增加实例心跳辅助判断，减少长任务被误恢复的概率。

### 4.1 优雅停止

调用 `Manager.Stop(ctx)` 时：

1. 取消 context，等待所有消费协程退出
2. **DelayQueue**：关闭内部 channel 后，在 **1 秒超时** 内将 channel 中残留的已转入 processing 但未执行的消息 Nack 回 pending
3. 超时后仍未处理完的消息保留在 processing 中，等待下次启动时的崩溃恢复机制兜底
4. **TimerTask**：等待正在执行的定时任务完成后才返回
5. 若 `ctx` 到期，`Stop(ctx)` 返回 `ctx.Err()`，调用方可据此决定是否继续等待

> **关于 DelayQueue 停止时的消息安全**：`fetch` 中 Lua 脚本会原子地将到期消息从 pending 转移到 processing，然后逐条投入 channel。如果 Stop 在 Lua 执行完成后、消息投入 channel 前触发，这部分消息会留在 processing 中而非 channel 中，因此 drain 阶段无法 Nack 回去。这些消息不会丢失，会在下次启动时由崩溃恢复机制兜底。当前设计优先保证停止速度，避免长时间阻塞导致 K8s 等容器编排系统强制杀死 pod。
>
> **后续优化方向**：可考虑在 `fetch` 中 ctx 取消时，将已取出但未投入 channel 的消息立即 Nack 回 pending，减少对崩溃恢复的依赖（默认等待 `processingTimeout=5min`）。

### 4.2 死信恢复边界

`RecoverEventDead` / `RecoverDelayDead` 当前是 best-effort 恢复：先从 dead 中弹出消息，在 Go 侧重置 `retry_count` 后写回 pending。如果弹出后进程退出、context 超时或 Redis 写 pending 失败，该条消息可能无法自动恢复。这个取舍保留了“恢复时重置重试计数”的语义；后续如果需要更强可靠性，可考虑“不弹出先复制”、Lua 原子恢复但不重写 Envelope，或引入 `recovering` 中间队列。

### 5. Valkey / Redis Cluster 兼容

所有 Redis key 使用 `{runner_name}` 作为 HashTag：

```
taskx:event:{order-notify}:pending
taskx:event:{order-notify}:processing
taskx:event:{order-notify}:dead
```

花括号内的部分用于计算 slot，同一 Runner 的所有 key 落在同一 slot，Lua 脚本操作多 key 时不会跨 slot。

> **最低版本要求**：Redis >= 6.2 / Valkey >= 7.0（需要 `BLMOVE` 命令支持）。启动时会自动检测版本，不满足时返回错误。

### 6. 多消费者

- **EventQueue**：配置 `ConsumerCount=N`，启动 N 个协程并发 `BLMOVE` 同一个 List，Redis 保证每条消息只被一个消费者取到
- **DelayQueue**：1 个轮询协程取出到期消息，投入 channel，N 个 worker 协程并发执行

### 7. 分布式锁

- **TimerTask**：每次 cron 触发时先抢锁；可按策略控制是否允许不同触发时刻重叠执行
- **崩溃恢复**：全局只有一台机器执行恢复
- 锁实现：`SET NX EX` + Lua 脚本安全释放（只释放自己持有的锁）

## 消息流转链路

### EventQueue

```
Producer                           Redis                              Consumer
   │                                 │                                    │
   │─── PublishEvent ──► LPUSH ────► pending ◄─── BLMOVE ────────────────│
   │                                 │              │                     │
   │                                 │              ▼                     │
   │                                 │          processing ──── Run() ───│
   │                                 │              │                     │
   │                                 │         ┌────┴────┐                │
   │                                 │         ▼         ▼                │
   │                                 │       成功       失败              │
   │                                 │      LREM    retry_count++        │
   │                                 │               │                    │
   │                                 │         ┌─────┴─────┐              │
   │                                 │         ▼           ▼              │
   │                                 │   ≤ MaxRetry   > MaxRetry         │
   │                                 │    回 pending    进 dead           │
```

如果 EventQueue 的 `Run()` 返回 `NextTime > 0`，框架不会自动转投 DelayQueue；它会触发 `OnAlert(AlertEventNextTimeIgnored)`，并直接 Ack 当前 event 消息，不再回到 event 重试链路。若需要指定下次执行时间，请由业务侧在告警回调中自行决定是否转投 DelayQueue。

### DelayQueue

```
Producer                           Redis                              Consumer
   │                                 │                                    │
   │── PublishDelay(t) ► ZADD ─────► pending(ZSet,score=t)               │
   │                                 │                                    │
   │                          [轮询: score <= now]                        │
   │                                 │                                    │
   │                                 ▼                                    │
   │                    Lua: ZREM pending + ZADD processing               │
   │                                 │                                    │
   │                                 ▼                                    │
   │                            processing ─── channel ──► worker.Run()  │
   │                                 │                         │          │
   │                            (同 EventQueue 的成功/失败流转)            │
```

### TimerTask

```
Cron 触发 ──► 按并发策略生成锁 Key ──► 抢分布式锁 ──► 获取成功 ──► Run() ──► 释放锁
                                │
                                └──► 获取失败 ──► 跳过（其他机器/其他轮次已占用）
```

支持两种并发策略：

- `forbid_overlap`：同一 task 上一轮未结束时，下一轮直接跳过
- `single_per_tick`：每个 cron tick 在集群内最多执行一次，不阻止不同触发时刻重叠执行

单个 task 可以通过 `RegisterTimerTask(..., TimerTaskOption{...})` 单独指定；未指定时继承 `WithDefaultTimerTaskOption(...)` 的全局默认值。

多机部署注意事项：

- `forbid_overlap`：框架会在任务执行期间自动续租锁，降低长任务因 `LockTTL` 到期而被其他实例重入的风险。
- `single_per_tick`：锁 key 基于“计划触发时间（cron tick）”生成，而不是简单使用 `time.Now()`，可降低同一轮任务因调度抖动产生重复执行的概率。
- 以上策略都仍依赖各实例机器时钟大体一致；若发生明显时钟漂移、Redis 不可用、长时间 STW/网络抖动等极端情况，仍可能出现重复执行，业务侧应保持幂等。

## 使用示例

示例已拆分到 `components/taskx/example/` 目录：

- `example/01_runner.go`：定义 Runner / TimerTask
- `example/02_bootstrap.go`：注册、启动、发布、优雅退出
- `example/03_recovery.go`：死信恢复
- `example/04_custom_driver.go`：自定义驱动接入
- `example/05_alert_notify.go`：接收 NextTime 告警并由业务转投 DelayQueue

### 直接投递 payload（新消息）

当业务已经拿到原始 payload，且希望不解析直接重新入队（会生成新的 Envelope ID）时，可使用：

```go
// 直接投递到 event
if env, err := mgr.PublishEventPayload(ctx, "event-runner-name", rawPayload); err != nil {
    return err
} else {
    log.Infof("event envelope id=%s", env.ID)
}

// 直接投递到 delay（executeAt 为秒级时间戳）
if env, err := mgr.PublishDelayPayload(ctx, "delay-runner-name", rawPayload, executeAt); err != nil {
    return err
} else {
    log.Infof("delay envelope id=%s", env.ID)
}
```

典型场景：EventQueue 中业务根据 `Run` 结果决定是否转 DelayQueue。  
可以直接把当前 `payload` 原样投递到 delay，避免重复解析/重组 payload。

## 配置参数

| Option | 默认值 | 说明 |
|--------|--------|------|
| `WithKeyPrefix(s)` | `"taskx"` | Redis key 前缀；HashTag 仍只使用 `{runner_name}` |
| `WithLogger(l)` | 无（必填） | `logger_factory.Logger` 实例 |
| `WithPollInterval(d)` | `1s` | DelayQueue 轮询间隔 |
| `WithLockTTL(d)` | `30s` | 分布式锁默认 TTL |
| `WithInternalOpTimeout(d)` | `3s` | 内部关键操作（如 Ack/RetryRequeue/MoveToDead/恢复锁续租与释放）使用的独立超时，避免受消费 ctx cancel 影响 |
| `WithTimerHeartbeatInterval(d)` | 自动计算（`min(HealthInterval, HealthBeatTimeout/2)`，下限 `1s`） | Timer 心跳上报间隔；用于避免 `HealthBeatTimeout` 较小时误判不健康 |
| `WithProcessingTimeout(d)` | `5m` | processing 队列超时时间，用于崩溃恢复等待 |
| `WithRecoverBatchSize(n)` | `1000` | 崩溃恢复每批次移动的消息数量 |
| `WithDefaultTimerTaskOption(opt)` | `MaxRetry=0, ConcurrencyPolicy=forbid_overlap` | TimerTask 全局默认选项，单任务未显式指定时继承 |
| `WithAlertFunc(f)` | `nil`（仅日志） | 异常告警回调（内部异步通知，不阻塞消费主流程） |
| `WithAlertQueueSize(n)` | `1024` | 内部告警通知通道容量，满时丢弃并记录日志 |
| `WithTraceContextKey(key)` | `"taskx_trace_id"` | 调用 `Run(ctx, payload)` 时注入 `Envelope.ID` 的 context key |
| `WithHealthInterval(d)` | `5s` | 健康监控采样间隔，控制 `HealthSnapshot()` 刷新频率 |
| `WithHealthBeatTimeout(d)` | `15s` | 心跳超时阈值，超过后对应监听器 `Alive=false` |

### TimerTaskOption

| 字段 | 默认值 | 说明 |
|------|--------|------|
| `MaxRetry` | 继承全局，最终默认 `0` | 执行失败重试次数 |
| `ConcurrencyPolicy` | 继承全局，最终默认 `forbid_overlap` | `forbid_overlap` / `single_per_tick` |

> 建议：关键任务默认使用 `forbid_overlap`；若诉求是“每个 tick 全局只执行一次”，使用 `single_per_tick`。无论使用哪种策略，业务侧都建议使用幂等键（如业务主键或 `task_name + tick`）做去重兜底。

## 监听存活监控

如果你只关心监听链路是否存活（不关心具体任务执行结果），可以使用 `HealthSnapshot()`：

- Event/Delay：基于内部轮询/消费心跳判断 `Alive`，并采样 pending 长度
- Timer：基于 cron 调度心跳判断 `Alive`

```go
mgr := taskx.NewRedisManager(
    rdb, reg,
    taskx.WithLogger(log),
    taskx.WithHealthInterval(2*time.Second),
    taskx.WithHealthBeatTimeout(10*time.Second),
)

if err := mgr.Start(ctx); err != nil {
    panic(err)
}
defer mgr.Stop(context.Background())

snap := mgr.HealthSnapshot()
for name, st := range snap.Event {
    log.Infof("event[%s]: alive=%v pending=%d err=%s", name, st.Alive, st.PendingLen, st.LenError)
}
for name, st := range snap.Delay {
    log.Infof("delay[%s]: alive=%v pending=%d err=%s", name, st.Alive, st.PendingLen, st.LenError)
}
log.Infof("timer: alive=%v last_beat=%v", snap.Timer.Alive, snap.Timer.LastBeatAt)
```

也可以直接用：

```go
if !mgr.HealthOK() {
    // 统一健康判定失败
}
```

## Redis Key 命名规范

```
{prefix}:event:{runner_name}:pending        # EventQueue 待消费
{prefix}:event:{runner_name}:processing     # EventQueue 执行中
{prefix}:event:{runner_name}:dead           # EventQueue 死信
{prefix}:delay:{runner_name}:pending        # DelayQueue 待触发
{prefix}:delay:{runner_name}:processing     # DelayQueue 执行中
{prefix}:delay:{runner_name}:dead           # DelayQueue 死信
{prefix}:lock:timer:{runner_name}           # TimerTask forbid_overlap 锁
{prefix}:lock:timer:{runner_name}:{slot}    # TimerTask single_per_tick 锁（slot 为 cron tick 时间窗）
{prefix}:lock:recover:event:{runner_name}   # EventQueue 恢复锁
{prefix}:lock:recover:delay:{runner_name}   # DelayQueue 恢复锁
```

> `{runner_name}` 中的花括号是 Redis HashTag 语法，确保同一 Runner 的所有 key 落在同一 slot，兼容 Valkey / Redis Cluster。
