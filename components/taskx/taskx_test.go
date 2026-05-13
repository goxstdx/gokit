package taskx_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/core"
	redisx "gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/provider/redis"
)

// ============================================================
// Redis 连接配置 —— 请根据实际环境填写
// ============================================================
func newTestRedis() redis.Cmdable {
	return redis.NewClient(
		&redis.Options{
			Addr:     "127.0.0.1:6379", // TODO: 填写 Redis 地址，如 "127.0.0.1:6379"
			Password: "",               // TODO: 填写密码
			DB:       10,
		},
	)
}

func newTestLogger(t *testing.T) logger_factory.Logger {
	l, err := logger_factory.NewLogger(
		logger_factory.Config{
			DriverType:  logger_factory.DriverZap,
			Level:       logger_factory.LevelDebug,
			Development: true,
		},
	)
	if err != nil {
		t.Fatalf("create logger: %v", err)
	}
	return l
}

// ============================================================
// 测试用 Runner 实现
// ============================================================
type testRunner struct {
	name    string
	payload string
	handler func(ctx context.Context, payload string) core.RunnerFuncResult

	mu       sync.Mutex
	received []string
}

func newTestRunner(name string, fn func(ctx context.Context, payload string) core.RunnerFuncResult) *testRunner {
	return &testRunner{name: name, handler: fn}
}

func (r *testRunner) GetName() string { return r.name }
func (r *testRunner) Marshal() string {
	return r.payload
}
func (r *testRunner) Run(ctx context.Context, payload string) core.RunnerFuncResult {
	r.mu.Lock()
	r.received = append(r.received, payload)
	r.mu.Unlock()
	if r.handler != nil {
		return r.handler(ctx, payload)
	}
	return core.RunnerFuncResult{IsOk: true}
}
func (r *testRunner) Received() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	cp := make([]string, len(r.received))
	copy(cp, r.received)
	return cp
}

type testTimerRunner struct {
	name    string
	cronExp string
	sleep   time.Duration
	count   atomic.Int64
}

func (r *testTimerRunner) GetName() string      { return r.name }
func (r *testTimerRunner) GetCron() string      { return r.cronExp }
func (r *testTimerRunner) GetTaskParam() string { return "" }
func (r *testTimerRunner) Run(_ context.Context, _ string) core.RunnerFuncResult {
	r.count.Add(1)
	if r.sleep > 0 {
		time.Sleep(r.sleep)
	}
	return core.RunnerFuncResult{IsOk: true}
}

type testConcurrentTimerRunner struct {
	name          string
	cronExp       string
	sleep         time.Duration
	inFlight      atomic.Int64
	maxConcurrent atomic.Int64
}

func (r *testConcurrentTimerRunner) GetName() string      { return r.name }
func (r *testConcurrentTimerRunner) GetCron() string      { return r.cronExp }
func (r *testConcurrentTimerRunner) GetTaskParam() string { return "" }
func (r *testConcurrentTimerRunner) Run(_ context.Context, _ string) core.RunnerFuncResult {
	cur := r.inFlight.Add(1)
	for {
		max := r.maxConcurrent.Load()
		if cur <= max || r.maxConcurrent.CompareAndSwap(max, cur) {
			break
		}
	}
	time.Sleep(r.sleep)
	r.inFlight.Add(-1)
	return core.RunnerFuncResult{IsOk: true}
}

// ============================================================
// 辅助函数
// ============================================================
func cleanKeys(ctx context.Context, rdb redis.Cmdable, prefix string) {
	if c, ok := rdb.(*redis.Client); ok {
		iter := c.Scan(ctx, 0, prefix+":*", 100).Iterator()
		for iter.Next(ctx) {
			c.Del(ctx, iter.Val())
		}
	}
}

func skipIfNoRedis(t *testing.T, rdb redis.Cmdable) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := rdb.(*redis.Client).Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}
}

// ============================================================
// 测试：Envelope 编解码
// ============================================================
func TestEnvelope(t *testing.T) {
	env := core.NewEnvelope("hello world", core.EnvelopeSourceEvent)
	env.RetryCount = 2

	encoded := env.Encode()
	decoded, err := core.DecodeEnvelope(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if decoded.Payload != "hello world" {
		t.Errorf("payload mismatch: got %q", decoded.Payload)
	}
	if decoded.RetryCount != 2 {
		t.Errorf("retry count: got %d, want 2", decoded.RetryCount)
	}
}

// ============================================================
// 测试：Registry 注册与去重
// ============================================================
func TestRegistryDuplicate(t *testing.T) {
	reg := taskx.NewRegistry()
	r := newTestRunner("dup-test", nil)

	if err := reg.RegisterEventRunner(r); err != nil {
		t.Fatalf("first register: %v", err)
	}
	if err := reg.RegisterEventRunner(r); err == nil {
		t.Fatal("expected duplicate registration error")
	}
}

// ============================================================
// 测试：EventQueue 完整流转（pending → processing → done / dead）
// ============================================================
func TestEventQueueFlow(t *testing.T) {
	rdb := newTestRedis()
	skipIfNoRedis(t, rdb)
	log := newTestLogger(t)

	ctx := context.Background()
	prefix := fmt.Sprintf("test_%d", time.Now().UnixNano())
	defer cleanKeys(ctx, rdb, prefix)

	var callCount atomic.Int64
	runner := newTestRunner(
		"evt-flow", func(_ context.Context, payload string) core.RunnerFuncResult {
			callCount.Add(1)
			if payload == "fail-always" {
				return core.RunnerFuncResult{IsOk: false, Err: fmt.Errorf("forced")}
			}
			return core.RunnerFuncResult{IsOk: true}
		},
	)

	reg := taskx.NewRegistry()
	if err := reg.RegisterEventRunner(runner, core.RunnerOption{MaxRetry: core.IntPtr(2), ConsumerCount: 1}); err != nil {
		t.Fatal(err)
	}

	mgr := taskx.NewRedisManager(
		rdb, reg,
		taskx.WithKeyPrefix(prefix),
		taskx.WithLogger(log),
		taskx.WithProcessingTimeout(time.Minute),
	)

	if err := mgr.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// 发送一条成功消息
	successRunner := newTestRunner("evt-flow", nil)
	successRunner.handler = func(_ context.Context, _ string) core.RunnerFuncResult {
		return core.RunnerFuncResult{IsOk: true}
	}

	ep := redisx.NewEventQueueProvider(rdb)
	env := core.NewEnvelope("success-msg", core.EnvelopeSourceEvent)
	pendingKey := prefix + ":event:{evt-flow}:pending"
	if err := ep.Push(ctx, pendingKey, env.Encode()); err != nil {
		t.Fatal(err)
	}

	// 发送一条总是失败的消息（应进入死信）
	envFail := core.NewEnvelope("fail-always", core.EnvelopeSourceEvent)
	if err := ep.Push(ctx, pendingKey, envFail.Encode()); err != nil {
		t.Fatal(err)
	}

	// 等待处理
	time.Sleep(5 * time.Second)

	if err := mgr.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	// 验证死信队列中有失败消息
	deadKey := prefix + ":event:{evt-flow}:dead"
	deadLen, err := rdb.LLen(ctx, deadKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if deadLen == 0 {
		t.Error("expected at least 1 message in dead letter queue")
	}

	// 验证成功消息被消费
	total := callCount.Load()
	t.Logf("total calls: %d, dead len: %d", total, deadLen)
	if total < 2 {
		t.Errorf("expected at least 2 calls (1 success + 1+ fail), got %d", total)
	}
}

// ============================================================
// 测试：DelayQueue 延迟执行
// ============================================================
func TestDelayQueueFlow(t *testing.T) {
	rdb := newTestRedis()
	skipIfNoRedis(t, rdb)
	log := newTestLogger(t)

	ctx := context.Background()
	prefix := fmt.Sprintf("test_%d", time.Now().UnixNano())
	defer cleanKeys(ctx, rdb, prefix)

	var executed atomic.Int64
	runner := newTestRunner(
		"dly-flow", func(_ context.Context, payload string) core.RunnerFuncResult {
			executed.Add(1)
			return core.RunnerFuncResult{IsOk: true}
		},
	)

	reg := taskx.NewRegistry()
	if err := reg.RegisterDelayRunner(runner, core.RunnerOption{MaxRetry: core.IntPtr(2), ConsumerCount: 2}); err != nil {
		t.Fatal(err)
	}

	mgr := taskx.NewRedisManager(
		rdb, reg,
		taskx.WithKeyPrefix(prefix),
		taskx.WithLogger(log),
		taskx.WithPollInterval(500*time.Millisecond),
	)

	if err := mgr.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// 发布 3 条延迟消息，延迟 1 秒
	dp := redisx.NewDelayQueueProvider(rdb)
	pendingKey := prefix + ":delay:{dly-flow}:pending"
	executeAt := time.Now().Add(time.Second).Unix()
	for i := 0; i < 3; i++ {
		env := core.NewEnvelope(fmt.Sprintf("delay-msg-%d", i), core.EnvelopeSourceDelay)
		if err := dp.Add(ctx, pendingKey, env.Encode(), executeAt); err != nil {
			t.Fatal(err)
		}
	}

	// 等待足够时间使消息到期并被消费
	time.Sleep(4 * time.Second)

	if err := mgr.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	got := executed.Load()
	if got != 3 {
		t.Errorf("expected 3 delayed messages executed, got %d", got)
	}
}

// ============================================================
// 测试：TimerTask + 分布式锁（两个 Manager 竞争同一个定时任务）
// ============================================================
func TestTimerTaskDistributedLock(t *testing.T) {
	rdb := newTestRedis()
	skipIfNoRedis(t, rdb)
	log := newTestLogger(t)

	ctx := context.Background()
	prefix := fmt.Sprintf("test_%d", time.Now().UnixNano())
	defer cleanKeys(ctx, rdb, prefix)

	var tasks []*testTimerRunner
	var managers []*taskx.Manager

	// 同一个定时任务，注册到两个 Manager（模拟两台机器）
	for i := 0; i < 2; i++ {
		task := &testTimerRunner{
			name:    "timer-lock-test",
			cronExp: "*/1 * * * * *", // 每秒执行
			sleep:   1200 * time.Millisecond,
		}
		// 让两个 runner 共享同一个计数器
		reg := taskx.NewRegistry()
		if err := reg.RegisterTimerTask(task); err != nil {
			t.Fatal(err)
		}

		mgr := taskx.NewRedisManager(
			rdb, reg,
			taskx.WithKeyPrefix(prefix),
			taskx.WithLogger(log),
			taskx.WithLockTTL(5*time.Second),
		)
		if err := mgr.Start(ctx); err != nil {
			t.Fatal(err)
		}

		tasks = append(tasks, task)
		managers = append(managers, mgr)
	}

	// 等待 3.5 秒，预期 cron 触发约 3 次
	time.Sleep(3500 * time.Millisecond)

	for _, mgr := range managers {
		if err := mgr.Stop(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// 停止后统计：每次只有一个实例能抢到锁执行
	// 总执行次数应 ≈ 3（而非 6），允许 ±1 的误差
	var total int64
	for _, task := range tasks {
		total += task.count.Load()
	}
	t.Logf("total timer executions across 2 managers: %d", total)
	// 两个实例都注册了每秒任务，3.5秒内触发 ~3次，如果锁有效则总执行次数 ≤ 4
	// 如果锁无效则两个实例各执行3次 = 6
	if total > 5 {
		t.Errorf("distributed lock not working: expected ~3 executions, got %d", total)
	}
}

func TestTimerTaskOptionFallbackAndOverride(t *testing.T) {
	rdb := newTestRedis()
	skipIfNoRedis(t, rdb)
	log := newTestLogger(t)

	ctx := context.Background()
	prefix := fmt.Sprintf("test_%d", time.Now().UnixNano())
	defer cleanKeys(ctx, rdb, prefix)

	globalTask := &testConcurrentTimerRunner{
		name:    "timer-global-single-per-tick",
		cronExp: "*/1 * * * * *",
		sleep:   1500 * time.Millisecond,
	}
	overrideTask := &testConcurrentTimerRunner{
		name:    "timer-override-forbid-overlap",
		cronExp: "*/1 * * * * *",
		sleep:   1500 * time.Millisecond,
	}

	reg := taskx.NewRegistry()
	if err := reg.RegisterTimerTask(globalTask); err != nil {
		t.Fatal(err)
	}
	if err := reg.RegisterTimerTask(
		overrideTask, core.TimerTaskOption{
			ConcurrencyPolicy: core.TimerConcurrencyPolicyPtr(core.TimerConcurrencyForbidOverlap),
		},
	); err != nil {
		t.Fatal(err)
	}

	mgr := taskx.NewRedisManager(
		rdb, reg,
		taskx.WithKeyPrefix(prefix),
		taskx.WithLogger(log),
		taskx.WithLockTTL(5*time.Second),
		taskx.WithDefaultTimerTaskOption(
			core.TimerTaskOption{
				ConcurrencyPolicy: core.TimerConcurrencyPolicyPtr(core.TimerConcurrencySinglePerTick),
			},
		),
	)
	if err := mgr.Start(ctx); err != nil {
		t.Fatal(err)
	}

	time.Sleep(3500 * time.Millisecond)

	if err := mgr.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	if got := globalTask.maxConcurrent.Load(); got < 2 {
		t.Fatalf("expected global default single_per_tick to permit overlapping runs across ticks, max concurrent=%d", got)
	}
	if got := overrideTask.maxConcurrent.Load(); got > 1 {
		t.Fatalf("expected per-task forbid_overlap to override global default, max concurrent=%d", got)
	}
}

func TestTimerTaskForbidOverlapRenewsLock(t *testing.T) {
	rdb := newTestRedis()
	skipIfNoRedis(t, rdb)
	log := newTestLogger(t)

	ctx := context.Background()
	prefix := fmt.Sprintf("test_%d", time.Now().UnixNano())
	defer cleanKeys(ctx, rdb, prefix)

	sharedTask := &testConcurrentTimerRunner{
		name:    "timer-renew-forbid-overlap",
		cronExp: "*/1 * * * * *",
		sleep:   2500 * time.Millisecond,
	}

	var managers []*taskx.Manager
	for i := 0; i < 2; i++ {
		reg := taskx.NewRegistry()
		if err := reg.RegisterTimerTask(
			sharedTask, core.TimerTaskOption{
				ConcurrencyPolicy: core.TimerConcurrencyPolicyPtr(core.TimerConcurrencyForbidOverlap),
			},
		); err != nil {
			t.Fatal(err)
		}

		mgr := taskx.NewRedisManager(
			rdb, reg,
			taskx.WithKeyPrefix(prefix),
			taskx.WithLogger(log),
			taskx.WithLockTTL(time.Second),
		)
		if err := mgr.Start(ctx); err != nil {
			t.Fatal(err)
		}
		managers = append(managers, mgr)
	}

	time.Sleep(4200 * time.Millisecond)

	for _, mgr := range managers {
		if err := mgr.Stop(ctx); err != nil {
			t.Fatal(err)
		}
	}

	if got := sharedTask.maxConcurrent.Load(); got > 1 {
		t.Fatalf("expected forbid_overlap with renew to prevent overlap across instances, max concurrent=%d", got)
	}
}

// ============================================================
// 测试：对外 Publish API 路径
// ============================================================
func TestPublishAPIs(t *testing.T) {
	rdb := newTestRedis()
	skipIfNoRedis(t, rdb)
	log := newTestLogger(t)

	ctx := context.Background()
	prefix := fmt.Sprintf("test_%d", time.Now().UnixNano())
	defer cleanKeys(ctx, rdb, prefix)

	var eventReceived atomic.Int64
	var delayReceived atomic.Int64

	eventRunner := newTestRunner(
		"publish-event", func(_ context.Context, payload string) core.RunnerFuncResult {
			if payload == "event-payload" {
				eventReceived.Add(1)
			}
			return core.RunnerFuncResult{IsOk: true}
		},
	)
	delayRunner := newTestRunner(
		"publish-delay", func(_ context.Context, payload string) core.RunnerFuncResult {
			if payload == "delay-payload" {
				delayReceived.Add(1)
			}
			return core.RunnerFuncResult{IsOk: true}
		},
	)

	reg := taskx.NewRegistry()
	if err := reg.RegisterEventRunner(eventRunner); err != nil {
		t.Fatal(err)
	}
	if err := reg.RegisterDelayRunner(delayRunner); err != nil {
		t.Fatal(err)
	}

	mgr := taskx.NewRedisManager(
		rdb, reg,
		taskx.WithKeyPrefix(prefix),
		taskx.WithLogger(log),
		taskx.WithPollInterval(200*time.Millisecond),
	)
	if err := mgr.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = mgr.Stop(ctx)
	}()

	if _, err := mgr.PublishEvent(ctx, &testRunner{name: "publish-event", payload: "event-payload"}); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.PublishDelay(
		ctx,
		&testRunner{name: "publish-delay", payload: "delay-payload"},
		time.Now().Add(time.Second).Unix(),
	); err != nil {
		t.Fatal(err)
	}

	time.Sleep(3 * time.Second)

	if got := eventReceived.Load(); got != 1 {
		t.Fatalf("expected PublishEvent message consumed once, got %d", got)
	}
	if got := delayReceived.Load(); got != 1 {
		t.Fatalf("expected PublishDelay message consumed once, got %d", got)
	}
}

// ============================================================
// 测试：优雅停止 — 确保 Stop 后消费者协程全部退出
// ============================================================
func TestGracefulShutdown(t *testing.T) {
	rdb := newTestRedis()
	skipIfNoRedis(t, rdb)
	log := newTestLogger(t)

	ctx := context.Background()
	prefix := fmt.Sprintf("test_%d", time.Now().UnixNano())
	defer cleanKeys(ctx, rdb, prefix)

	var processing atomic.Int64
	var completed atomic.Int64
	runner := newTestRunner(
		"shutdown-test", func(_ context.Context, payload string) core.RunnerFuncResult {
			processing.Add(1)
			time.Sleep(2 * time.Second)
			completed.Add(1)
			return core.RunnerFuncResult{IsOk: true}
		},
	)

	reg := taskx.NewRegistry()
	if err := reg.RegisterEventRunner(runner, core.RunnerOption{MaxRetry: core.IntPtr(1), ConsumerCount: 2}); err != nil {
		t.Fatal(err)
	}

	mgr := taskx.NewRedisManager(
		rdb, reg,
		taskx.WithKeyPrefix(prefix),
		taskx.WithLogger(log),
	)

	if err := mgr.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// 推入两条消息，让两个消费者各处理一条
	ep := redisx.NewEventQueueProvider(rdb)
	pendingKey := prefix + ":event:{shutdown-test}:pending"
	for i := 0; i < 2; i++ {
		env := core.NewEnvelope(fmt.Sprintf("msg-%d", i), core.EnvelopeSourceEvent)
		if err := ep.Push(ctx, pendingKey, env.Encode()); err != nil {
			t.Fatal(err)
		}
	}

	// 等待消费者开始处理（handler 内 sleep 2s）
	time.Sleep(500 * time.Millisecond)

	inFlight := processing.Load()
	t.Logf("in-flight tasks before stop: %d", inFlight)

	// 发起优雅停止 —— 应该等待 handler 返回才结束
	stopStart := time.Now()
	if err := mgr.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	stopDuration := time.Since(stopStart)

	t.Logf("stop took %v, processing=%d, completed=%d", stopDuration, processing.Load(), completed.Load())

	// handler sleep 2s，我们 0.5s 后 Stop，Stop 应至少等 ~1.5s
	if inFlight > 0 && stopDuration < time.Second {
		t.Errorf("Stop returned too quickly (%v), did not wait for in-flight tasks", stopDuration)
	}

	// 所有已开始的任务应该完成
	if completed.Load() < inFlight {
		t.Errorf("not all in-flight tasks completed: started=%d, completed=%d", inFlight, completed.Load())
	}
}

// ============================================================
// 测试：Redis 分布式锁 — 互斥性、释放、续期
// ============================================================
func TestRedisLock(t *testing.T) {
	rdb := newTestRedis()
	skipIfNoRedis(t, rdb)

	ctx := context.Background()
	prefix := fmt.Sprintf("test_%d", time.Now().UnixNano())
	defer cleanKeys(ctx, rdb, prefix)

	lp := redisx.NewLockProvider(rdb)
	lockKey := prefix + ":lock:{test-lock}"

	// 第一次加锁应成功
	ok, err := lp.Lock(ctx, lockKey, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("first lock should succeed")
	}

	// 第二次加锁应失败（互斥）
	ok2, err := lp.Lock(ctx, lockKey, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok2 {
		t.Fatal("second lock should fail (mutex)")
	}

	// 另一个 provider 也无法获取同一把锁
	lp2 := redisx.NewLockProvider(rdb)
	ok3, err := lp2.Lock(ctx, lockKey, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if ok3 {
		t.Fatal("different provider should not acquire held lock")
	}

	// 续期应成功
	renewed, err := lp.Renew(ctx, lockKey, 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !renewed {
		t.Fatal("renew should succeed for lock holder")
	}

	// 另一个 provider 续期应失败
	renewed2, err := lp2.Renew(ctx, lockKey, 20*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if renewed2 {
		t.Fatal("renew should fail for non-holder")
	}

	// 释放锁
	if err := lp.Unlock(ctx, lockKey); err != nil {
		t.Fatal(err)
	}

	// 释放后另一个 provider 应该能获取
	ok4, err := lp2.Lock(ctx, lockKey, 10*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if !ok4 {
		t.Fatal("lock should be available after unlock")
	}

	_ = lp2.Unlock(ctx, lockKey)
}

// ============================================================
// 测试：分布式锁竞争 — 多协程并发抢锁
// ============================================================
func TestRedisLockConcurrency(t *testing.T) {
	rdb := newTestRedis()
	skipIfNoRedis(t, rdb)

	ctx := context.Background()
	prefix := fmt.Sprintf("test_%d", time.Now().UnixNano())
	defer cleanKeys(ctx, rdb, prefix)

	lockKey := prefix + ":lock:{concurrency}"
	const goroutines = 10
	var winners atomic.Int64

	var wg sync.WaitGroup
	wg.Add(goroutines)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			lp := redisx.NewLockProvider(rdb)
			ok, err := lp.Lock(ctx, lockKey, 10*time.Second)
			if err != nil {
				t.Errorf("lock error: %v", err)
				return
			}
			if ok {
				winners.Add(1)
				time.Sleep(100 * time.Millisecond)
				_ = lp.Unlock(ctx, lockKey)
			}
		}()
	}
	wg.Wait()

	// 同一时刻只有一个 goroutine 能获取锁
	w := winners.Load()
	t.Logf("winners: %d / %d", w, goroutines)
	if w != 1 {
		t.Errorf("expected exactly 1 winner, got %d", w)
	}
}

// ============================================================
// 测试：死信队列恢复
// ============================================================
func TestDeadLetterRecovery(t *testing.T) {
	rdb := newTestRedis()
	skipIfNoRedis(t, rdb)
	log := newTestLogger(t)

	ctx := context.Background()
	prefix := fmt.Sprintf("test_%d", time.Now().UnixNano())
	defer cleanKeys(ctx, rdb, prefix)

	failCount := atomic.Int64{}
	runner := newTestRunner(
		"dlq-recover", func(_ context.Context, payload string) core.RunnerFuncResult {
			n := failCount.Add(1)
			// 前 3 次全部失败（MaxRetry=2 意味着重试 2 次后进死信）
			if n <= 3 {
				return core.RunnerFuncResult{IsOk: false, Err: fmt.Errorf("fail #%d", n)}
			}
			return core.RunnerFuncResult{IsOk: true}
		},
	)

	reg := taskx.NewRegistry()
	if err := reg.RegisterEventRunner(runner, core.RunnerOption{MaxRetry: core.IntPtr(2), ConsumerCount: 1}); err != nil {
		t.Fatal(err)
	}

	mgr := taskx.NewRedisManager(
		rdb, reg,
		taskx.WithKeyPrefix(prefix),
		taskx.WithLogger(log),
	)

	if err := mgr.Start(ctx); err != nil {
		t.Fatal(err)
	}

	ep := redisx.NewEventQueueProvider(rdb)
	pendingKey := prefix + ":event:{dlq-recover}:pending"
	env := core.NewEnvelope("will-fail-then-succeed", core.EnvelopeSourceEvent)
	if err := ep.Push(ctx, pendingKey, env.Encode()); err != nil {
		t.Fatal(err)
	}

	// 等待消息被消费并进入死信
	time.Sleep(5 * time.Second)

	deadKey := prefix + ":event:{dlq-recover}:dead"
	deadLen, _ := rdb.LLen(ctx, deadKey).Result()
	t.Logf("dead letter count before recovery: %d", deadLen)
	if deadLen == 0 {
		t.Fatal("expected message in dead letter queue")
	}

	// 恢复死信
	recovered, err := mgr.RecoverEventDead(ctx, "dlq-recover", 100)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("recovered %d messages from dead letter", recovered)
	if recovered == 0 {
		t.Fatal("expected to recover at least 1 message")
	}

	// 恢复后等消费（此时 handler 会返回成功）
	time.Sleep(3 * time.Second)

	if err := mgr.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	// 死信应为空
	deadLenAfter, _ := rdb.LLen(ctx, deadKey).Result()
	if deadLenAfter != 0 {
		t.Errorf("dead letter should be empty after recovery, got %d", deadLenAfter)
	}
}

// ============================================================
// 测试：Manager 重复启动 / 未启动停止
// ============================================================
func TestManagerLifecycle(t *testing.T) {
	rdb := newTestRedis()
	skipIfNoRedis(t, rdb)
	log := newTestLogger(t)

	ctx := context.Background()
	prefix := fmt.Sprintf("test_%d", time.Now().UnixNano())
	defer cleanKeys(ctx, rdb, prefix)

	reg := taskx.NewRegistry()
	if err := reg.RegisterEventRunner(newTestRunner("lifecycle-runner", nil)); err != nil {
		t.Fatal(err)
	}
	mgr := taskx.NewRedisManager(
		rdb, reg,
		taskx.WithKeyPrefix(prefix),
		taskx.WithLogger(log),
	)

	// 未启动时 Stop 不报错
	if err := mgr.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	if err := mgr.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// 重复启动应报错
	if err := mgr.Start(ctx); err == nil {
		t.Fatal("expected error on double start")
	}

	if err := mgr.Stop(ctx); err != nil {
		t.Fatal(err)
	}
}

// ============================================================
// 测试：Logger 必须配置
// ============================================================
func TestManagerRequiresLogger(t *testing.T) {
	rdb := newTestRedis()
	skipIfNoRedis(t, rdb)

	ctx := context.Background()
	reg := taskx.NewRegistry()
	mgr := taskx.NewRedisManager(rdb, reg) // 没有 WithLogger

	err := mgr.Start(ctx)
	if err == nil {
		_ = mgr.Stop(ctx)
		t.Fatal("expected error when logger not set")
	}
	t.Logf("got expected error: %v", err)
}

// ============================================================
// 测试：监听存活健康快照（event / delay / timer）
// ============================================================
func TestManagerHealthSnapshot(t *testing.T) {
	rdb := newTestRedis()
	skipIfNoRedis(t, rdb)
	log := newTestLogger(t)

	ctx := context.Background()
	prefix := fmt.Sprintf("test_%d", time.Now().UnixNano())
	defer cleanKeys(ctx, rdb, prefix)

	eventRunner := newTestRunner(
		"health-event", func(_ context.Context, _ string) core.RunnerFuncResult {
			return core.RunnerFuncResult{IsOk: true}
		},
	)
	delayRunner := newTestRunner(
		"health-delay", func(_ context.Context, _ string) core.RunnerFuncResult {
			return core.RunnerFuncResult{IsOk: true}
		},
	)
	timerRunner := &testTimerRunner{name: "health-timer", cronExp: "*/1 * * * * *"}

	reg := taskx.NewRegistry()
	if err := reg.RegisterEventRunner(eventRunner, core.RunnerOption{ConsumerCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := reg.RegisterDelayRunner(delayRunner, core.RunnerOption{ConsumerCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := reg.RegisterTimerTask(timerRunner); err != nil {
		t.Fatal(err)
	}

	mgr := taskx.NewRedisManager(
		rdb, reg,
		taskx.WithKeyPrefix(prefix),
		taskx.WithLogger(log),
		taskx.WithPollInterval(200*time.Millisecond),
		taskx.WithHealthInterval(100*time.Millisecond),
		taskx.WithHealthBeatTimeout(6*time.Second),
	)

	if err := mgr.Start(ctx); err != nil {
		t.Fatal(err)
	}

	// 推入一条未来执行的 delay 消息，验证 pending 长度采样。
	if _, err := mgr.PublishDelay(
		ctx,
		&testRunner{name: "health-delay", payload: "future-job"},
		time.Now().Add(30*time.Second).Unix(),
	); err != nil {
		t.Fatal(err)
	}

	var snap taskx.ManagerHealthSnapshot
	ok := false
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		snap = mgr.HealthSnapshot()
		eventState, hasEvent := snap.Event["health-event"]
		delayState, hasDelay := snap.Delay["health-delay"]
		if snap.Running &&
			hasEvent && hasDelay &&
			eventState.Alive && delayState.Alive &&
			snap.Timer.Alive &&
			delayState.PendingLen == 1 &&
			eventState.LenError == "" && delayState.LenError == "" {
			ok = true
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("unexpected health snapshot: %+v", snap)
	}

	if err := mgr.Stop(ctx); err != nil {
		t.Fatal(err)
	}

	stopped := mgr.HealthSnapshot()
	if stopped.Running {
		t.Fatalf("expected running=false after stop, got %+v", stopped)
	}
}

// ============================================================
// 测试：业务侧可将 event payload 作为新消息直接转投 delay
// ============================================================
func TestEventPayloadDirectRepublishToDelay(t *testing.T) {
	rdb := newTestRedis()
	skipIfNoRedis(t, rdb)
	log := newTestLogger(t)

	ctx := context.Background()
	prefix := fmt.Sprintf("test_%d", time.Now().UnixNano())
	defer cleanKeys(ctx, rdb, prefix)

	var mgr *taskx.Manager
	gotDelayPayload := make(chan string, 1)

	eventRunner := newTestRunner(
		"evt-direct", func(ctx context.Context, payload string) core.RunnerFuncResult {
			nextTime := time.Now().Add(1 * time.Second).Unix()
			if _, err := mgr.PublishDelayPayload(ctx, "delay-direct", payload, nextTime); err != nil {
				return core.RunnerFuncResult{IsOk: false, Err: err}
			}
			return core.RunnerFuncResult{IsOk: true}
		},
	)
	delayRunner := newTestRunner(
		"delay-direct", func(_ context.Context, payload string) core.RunnerFuncResult {
			select {
			case gotDelayPayload <- payload:
			default:
			}
			return core.RunnerFuncResult{IsOk: true}
		},
	)

	reg := taskx.NewRegistry()
	if err := reg.RegisterEventRunner(eventRunner, core.RunnerOption{ConsumerCount: 1}); err != nil {
		t.Fatal(err)
	}
	if err := reg.RegisterDelayRunner(delayRunner, core.RunnerOption{ConsumerCount: 1}); err != nil {
		t.Fatal(err)
	}

	mgr = taskx.NewRedisManager(
		rdb, reg,
		taskx.WithKeyPrefix(prefix),
		taskx.WithLogger(log),
		taskx.WithPollInterval(100*time.Millisecond),
	)
	if err := mgr.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mgr.Stop(context.Background()) }()

	rawPayload := `{"order_id":"o123","trace_id":"t999","retry":1}`
	if _, err := mgr.PublishEventPayload(ctx, "evt-direct", rawPayload); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-gotDelayPayload:
		if got != rawPayload {
			t.Fatalf("payload changed after republish, got=%q want=%q", got, rawPayload)
		}
	case <-time.After(6 * time.Second):
		t.Fatal("timeout waiting for delay payload")
	}
}

// ============================================================
// 测试：Event 返回 NextTime 后仅告警并 Ack，不再回 event 重试
// ============================================================
func TestEventNextTimeAlertAndNoRetry(t *testing.T) {
	rdb := newTestRedis()
	skipIfNoRedis(t, rdb)
	log := newTestLogger(t)

	ctx := context.Background()
	prefix := fmt.Sprintf("test_%d", time.Now().UnixNano())
	defer cleanKeys(ctx, rdb, prefix)

	var runCount atomic.Int64
	alertCh := make(chan core.AlertData, 4)
	runner := newTestRunner(
		"evt-next-time", func(_ context.Context, _ string) core.RunnerFuncResult {
			runCount.Add(1)
			return core.RunnerFuncResult{
				IsOk:     false,
				Err:      fmt.Errorf("need reschedule"),
				NextTime: time.Now().Add(5 * time.Second).Unix(),
			}
		},
	)

	reg := taskx.NewRegistry()
	if err := reg.RegisterEventRunner(runner, core.RunnerOption{MaxRetry: core.IntPtr(3), ConsumerCount: 1}); err != nil {
		t.Fatal(err)
	}

	mgr := taskx.NewRedisManager(
		rdb, reg,
		taskx.WithKeyPrefix(prefix),
		taskx.WithLogger(log),
		taskx.WithAlertFunc(
			func(data core.AlertData) {
				select {
				case alertCh <- data:
				default:
				}
			},
		),
	)
	if err := mgr.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = mgr.Stop(context.Background()) }()

	env, err := mgr.PublishEventPayload(ctx, "evt-next-time", "payload-1")
	if err != nil {
		t.Fatal(err)
	}

	var alert core.AlertData
	select {
	case alert = <-alertCh:
	case <-time.After(4 * time.Second):
		t.Fatal("timeout waiting next-time alert")
	}
	if alert.AlertType != core.AlertEventNextTimeIgnored {
		t.Fatalf("unexpected alert type: %s", alert.AlertType)
	}
	if alert.Envelope == nil || alert.Envelope.ID != env.ID {
		t.Fatalf("alert envelope mismatch: got=%+v want_id=%s", alert.Envelope, env.ID)
	}

	// 额外等待，确认不会再次重试。
	time.Sleep(2 * time.Second)
	if got := runCount.Load(); got != 1 {
		t.Fatalf("expected run count=1, got=%d", got)
	}

	pendingKey := prefix + ":event:{evt-next-time}:pending"
	processingKey := prefix + ":event:{evt-next-time}:processing"
	pendingLen, err := rdb.LLen(ctx, pendingKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	processingLen, err := rdb.LLen(ctx, processingKey).Result()
	if err != nil {
		t.Fatal(err)
	}
	if pendingLen != 0 || processingLen != 0 {
		t.Fatalf("expected event queue empty after next-time ack, pending=%d processing=%d", pendingLen, processingLen)
	}
}
