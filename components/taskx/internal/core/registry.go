package core

import (
	"fmt"
	"sync"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/defaults"
)

// EventEntry 事件队列注册条目
type EventEntry struct {
	Runner QueueRunner
	Option RunnerOption
}

// DelayEntry 延迟队列注册条目
type DelayEntry struct {
	Runner QueueRunner
	Option RunnerOption
}

// TimerEntry 定时任务注册条目
type TimerEntry struct {
	Task   TimerTaskRunner
	Option TimerTaskOption
}

// Registry 统一注册中心
type Registry struct {
	mu           sync.RWMutex
	eventRunners map[string]*EventEntry
	delayRunners map[string]*DelayEntry
	timerTasks   map[string]*TimerEntry
	frozen       bool
}

func (r *Registry) IsHasRunner() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.eventRunners == nil && r.delayRunners == nil && r.timerTasks == nil {
		return false
	}
	return len(r.eventRunners) > 0 || len(r.delayRunners) > 0 || len(r.timerTasks) > 0
}

func NewRegistry() *Registry {
	return &Registry{
		eventRunners: make(map[string]*EventEntry),
		delayRunners: make(map[string]*DelayEntry),
		timerTasks:   make(map[string]*TimerEntry),
	}
}

func GetDefaultEventOption() RunnerOption {
	return RunnerOption{MaxRetry: defaults.EventMaxRetry, ConsumerCount: defaults.EventConsumerCount}
}

// RegisterEventRunner 注册事件队列 Runner
func (r *Registry) RegisterEventRunner(runner QueueRunner, opts ...RunnerOption) error {
	opt := GetDefaultEventOption()
	if len(opts) > 0 {
		opt = opts[0]
	}
	opt = opt.Normalize()

	name := runner.GetName()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return fmt.Errorf("taskx: registry is frozen and cannot be modified")
	}

	if _, exists := r.eventRunners[name]; exists {
		return fmt.Errorf("taskx: event runner %q already registered", name)
	}
	r.eventRunners[name] = &EventEntry{Runner: runner, Option: opt}
	return nil
}

func GetDefaultDelayOption() RunnerOption {
	return RunnerOption{MaxRetry: defaults.DelayMaxRetry, ConsumerCount: defaults.DelayConsumerCount}
}

// RegisterDelayRunner 注册延迟队列 Runner
func (r *Registry) RegisterDelayRunner(runner QueueRunner, opts ...RunnerOption) error {
	opt := GetDefaultDelayOption()
	if len(opts) > 0 {
		opt = opts[0]
	}
	opt = opt.Normalize()

	name := runner.GetName()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return fmt.Errorf("taskx: registry is frozen and cannot be modified")
	}

	if _, exists := r.delayRunners[name]; exists {
		return fmt.Errorf("taskx: delay runner %q already registered", name)
	}
	r.delayRunners[name] = &DelayEntry{Runner: runner, Option: opt}
	return nil
}

func GetDefaultTimerTaskOption() TimerTaskOption {
	return TimerTaskOption{}
}

// RegisterTimerTask 注册定时任务
func (r *Registry) RegisterTimerTask(task TimerTaskRunner, opts ...TimerTaskOption) error {
	opt := TimerTaskOption{}
	if len(opts) > 0 {
		opt = opts[0]
	}
	opt = opt.Normalize()

	name := task.GetName()
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.frozen {
		return fmt.Errorf("taskx: registry is frozen and cannot be modified")
	}

	if _, exists := r.timerTasks[name]; exists {
		return fmt.Errorf("taskx: timer task %q already registered", name)
	}
	r.timerTasks[name] = &TimerEntry{Task: task, Option: opt}
	return nil
}

// GetEventRunners 获取所有已注册的事件队列 Runner（快照）
func (r *Registry) GetEventRunners() map[string]*EventEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cp := make(map[string]*EventEntry, len(r.eventRunners))
	for k, v := range r.eventRunners {
		if v == nil {
			cp[k] = nil
			continue
		}
		entryCopy := *v
		cp[k] = &entryCopy
	}
	return cp
}

// GetDelayRunners 获取所有已注册的延迟队列 Runner（快照）
func (r *Registry) GetDelayRunners() map[string]*DelayEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cp := make(map[string]*DelayEntry, len(r.delayRunners))
	for k, v := range r.delayRunners {
		if v == nil {
			cp[k] = nil
			continue
		}
		entryCopy := *v
		cp[k] = &entryCopy
	}
	return cp
}

// GetTimerTasks 获取所有已注册的定时任务（快照）
func (r *Registry) GetTimerTasks() map[string]*TimerEntry {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cp := make(map[string]*TimerEntry, len(r.timerTasks))
	for k, v := range r.timerTasks {
		if v == nil {
			cp[k] = nil
			continue
		}
		entryCopy := *v
		cp[k] = &entryCopy
	}
	return cp
}

// Snapshot 返回当前注册表的只读深拷贝快照，供外部查看使用。
func (r *Registry) Snapshot() *Registry {
	if r == nil {
		return nil
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	snapshot := &Registry{
		eventRunners: make(map[string]*EventEntry, len(r.eventRunners)),
		delayRunners: make(map[string]*DelayEntry, len(r.delayRunners)),
		timerTasks:   make(map[string]*TimerEntry, len(r.timerTasks)),
		frozen:       true,
	}
	for k, v := range r.eventRunners {
		if v == nil {
			snapshot.eventRunners[k] = nil
			continue
		}
		entryCopy := *v
		snapshot.eventRunners[k] = &entryCopy
	}
	for k, v := range r.delayRunners {
		if v == nil {
			snapshot.delayRunners[k] = nil
			continue
		}
		entryCopy := *v
		snapshot.delayRunners[k] = &entryCopy
	}
	for k, v := range r.timerTasks {
		if v == nil {
			snapshot.timerTasks[k] = nil
			continue
		}
		entryCopy := *v
		snapshot.timerTasks[k] = &entryCopy
	}
	return snapshot
}

func (r *Registry) Freeze() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.frozen = true
}

func (r *Registry) Unfreeze() {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.frozen = false
}

// EventGroupResolver 返回一个解析函数，根据 runnerName 返回 (groupName, registered)。
func (r *Registry) EventGroupResolver() func(runnerName string) (groupName string, registered bool) {
	return func(runnerName string) (string, bool) {
		entries := r.GetEventRunners()
		if entry, ok := entries[runnerName]; ok {
			if entry.Option.QueueGroup != "" {
				return entry.Option.QueueGroup, true
			}
			return DefaultEventQueueGroup, true
		}
		return runnerName, false
	}
}

// DelayRegisteredChecker 返回一个检查函数，判断 delay runner 是否已注册。
func (r *Registry) DelayRegisteredChecker() func(runnerName string) bool {
	return func(runnerName string) bool {
		entries := r.GetDelayRunners()
		_, ok := entries[runnerName]
		return ok
	}
}
