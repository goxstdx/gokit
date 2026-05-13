# taskx — 统一任务队列与定时任务框架

## 概述

`taskx` 是一个面向多机部署的任务调度框架，提供三种能力：

| 能力 | 说明 | 底层结构（Redis） |
|------|------|------------------|
| **EventQueue** | 实时事件队列，生产即消费 | List（LPUSH / BRPOP） |
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
- **processing → dead**：超过 `MaxRetry` 进入死信队列
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

重试时 `retry_count` 递增，达到 `MaxRetry` 后进入死信。死信恢复时 `retry_count` 重置为 0。

### 4. 崩溃恢复

启动时会启一个一次性恢复协程：

```
抢分布式锁 → 等待 processingTimeout → 将 processing 残留消息移回 pending → 释放锁 → 退出
```

- **等待 processingTimeout**：给活跃的消费者足够时间处理完，避免误恢复正在执行的消息
- **分布式锁**：多机启动时只有一台执行恢复
- 等待结束后 processing 中还残留的消息一定是崩溃遗留的，可以安全恢复

### 4.1 优雅停止

调用 `Manager.Stop()` 时：

1. 取消 context，等待所有消费协程退出
2. **DelayQueue**：关闭内部 channel 后，在 **1 秒超时** 内将 channel 中残留的已转入 processing 但未执行的消息 Nack 回 pending
3. 超时后仍未处理完的消息保留在 processing 中，等待下次启动时的崩溃恢复机制兜底
4. **TimerTask**：等待正在执行的定时任务完成后才返回

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

- **TimerTask**：每次 cron 触发时先抢锁，只有一台机器执行
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
   │                                 │   < MaxRetry   >= MaxRetry        │
   │                                 │    回 pending    进 dead           │
```

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
Cron 触发 ──► 抢分布式锁 ──► 获取成功 ──► Run() ──► 释放锁
                  │
                  └──► 获取失败 ──► 跳过（其他机器在执行）
```

## 使用示例

### 1. 定义 Runner

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"

    "gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/core"
)

// OrderNotifyRunner 订单通知 Runner（EventQueue 用）
type OrderNotifyRunner struct {
    OrderID string `json:"order_id"`
    UserID  string `json:"user_id"`
}

func (r *OrderNotifyRunner) GetName() string { return "order-notify" }

func (r *OrderNotifyRunner) Marshal(runner core.QueueRunner) string {
    b, _ := json.Marshal(runner)
    return string(b)
}

func (r *OrderNotifyRunner) Unmarshal(val string, obj core.QueueRunner) error {
    return json.Unmarshal([]byte(val), obj)
}

func (r *OrderNotifyRunner) Run(ctx context.Context, payload string) core.RunnerFuncResult {
    var data OrderNotifyRunner
    if err := json.Unmarshal([]byte(payload), &data); err != nil {
        return core.RunnerFuncResult{IsOk: false, Err: err}
    }
    fmt.Printf("sending notification: order=%s, user=%s\n", data.OrderID, data.UserID)
    // ... 发送通知逻辑 ...
    return core.RunnerFuncResult{IsOk: true}
}

// ReportTimerTask 定时报表任务（TimerTask 用）
type ReportTimerTask struct{}

func (t *ReportTimerTask) GetName() string { return "daily-report" }
func (t *ReportTimerTask) GetCron() string { return "0 0 2 * * *" } // 每天凌晨 2 点

func (t *ReportTimerTask) Run(ctx context.Context) core.RunnerFuncResult {
    fmt.Println("generating daily report...")
    return core.RunnerFuncResult{IsOk: true}
}
```

### 2. 注册并启动

```go
package main

import (
    "context"
    "os"
    "os/signal"
    "syscall"
    "time"

    "github.com/redis/go-redis/v9"

    "gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
    "gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx"
    "gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/core"
)

func main() {
    // 初始化 Logger
    log, _ := logger_factory.NewLogger(logger_factory.Config{
        DriverType:  logger_factory.DriverZap,
        Level:       logger_factory.LevelInfo,
        Development: true,
    })

    // 初始化 Redis
    rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

    // 创建注册中心
    registry := taskx.NewRegistry()

    // 注册 EventQueue Runner（3 个消费者，最大重试 5 次）
    _ = registry.RegisterEventRunner(
        &OrderNotifyRunner{},
        core.RunnerOption{MaxRetry: core.IntPtr(5), ConsumerCount: 3},
    )

    // 注册 DelayQueue Runner
    _ = registry.RegisterDelayRunner(
        &OrderNotifyRunner{},
        core.RunnerOption{MaxRetry: core.IntPtr(3), ConsumerCount: 2},
    )

    // 注册 TimerTask
    _ = registry.RegisterTimerTask(&ReportTimerTask{})

    // 创建 Manager
    mgr := taskx.NewRedisManager(rdb, registry,
        taskx.WithKeyPrefix("myapp"),
        taskx.WithLogger(log),
        taskx.WithPollInterval(time.Second),
        taskx.WithLockTTL(30*time.Second),
        taskx.WithProcessingTimeout(5*time.Minute),
    )

    // 启动
    ctx := context.Background()
    if err := mgr.Start(ctx); err != nil {
        panic(err)
    }

    // 发布事件
    _ = mgr.PublishEvent(ctx, &OrderNotifyRunner{
        OrderID: "ORD-001",
        UserID:  "USR-123",
    })

    // 发布延迟任务（10 分钟后执行）
    _ = mgr.PublishDelay(ctx, &OrderNotifyRunner{
        OrderID: "ORD-002",
        UserID:  "USR-456",
    }, time.Now().Add(10*time.Minute).Unix())

    // 优雅退出
    quit := make(chan os.Signal, 1)
    signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
    <-quit

    _ = mgr.Stop(ctx)
}
```

### 3. 死信恢复

```go
// 恢复事件队列死信（最多 100 条），RetryCount 会被重置为 0
recovered, err := mgr.RecoverEventDead(ctx, "order-notify", 100)

// 恢复延迟队列死信
recovered, err := mgr.RecoverDelayDead(ctx, "order-notify", 100)
```

### 4. 自定义驱动

实现 `driver/` 下的接口即可替换 Redis：

```go
import "gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/driver"

type MyEventQueueDriver struct{ /* ... */ }
// 实现 driver.EventQueueDriver 接口的所有方法

mgr := taskx.NewManager(registry,
    taskx.WithEventQueueDriver(&MyEventQueueDriver{}),
    taskx.WithDelayQueueDriver(&MyDelayQueueDriver{}),
    taskx.WithLockDriver(&MyLockDriver{}),
    taskx.WithLogger(log),
)
// 需要自行设置 ConsumerFactory，或使用自定义 Manager 封装
```

## 配置参数

| Option | 默认值 | 说明 |
|--------|--------|------|
| `WithKeyPrefix(s)` | `"taskx"` | Redis key 前缀（会作为 HashTag 的一部分） |
| `WithLogger(l)` | 无（必填） | `logger_factory.Logger` 实例 |
| `WithPollInterval(d)` | `1s` | DelayQueue 轮询间隔 |
| `WithLockTTL(d)` | `30s` | 分布式锁默认 TTL |
| `WithProcessingTimeout(d)` | `5m` | processing 队列超时时间，用于崩溃恢复等待 |
| `WithRecoverBatchSize(n)` | `1000` | 崩溃恢复每批次移动的消息数量 |

## Redis Key 命名规范

```
{prefix}:event:{runner_name}:pending        # EventQueue 待消费
{prefix}:event:{runner_name}:processing     # EventQueue 执行中
{prefix}:event:{runner_name}:dead           # EventQueue 死信
{prefix}:delay:{runner_name}:pending        # DelayQueue 待触发
{prefix}:delay:{runner_name}:processing     # DelayQueue 执行中
{prefix}:delay:{runner_name}:dead           # DelayQueue 死信
{prefix}:lock:timer:{runner_name}           # TimerTask 执行锁
{prefix}:lock:recover:event:{runner_name}   # EventQueue 恢复锁
{prefix}:lock:recover:delay:{runner_name}   # DelayQueue 恢复锁
```

> `{runner_name}` 中的花括号是 Redis HashTag 语法，确保同一 Runner 的所有 key 落在同一 slot，兼容 Valkey / Redis Cluster。
