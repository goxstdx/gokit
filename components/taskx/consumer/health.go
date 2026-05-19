package consumer

import (
	"context"
	"fmt"
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/defaults"
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/queue"
)

// HealthSnapshot 返回最近一次健康快照。
func (c *Consumer) HealthSnapshot() HealthSnapshot {
	c.healthMu.RLock()
	defer c.healthMu.RUnlock()

	cp := HealthSnapshot{
		Running:   c.healthSnapshot.Running,
		CheckedAt: c.healthSnapshot.CheckedAt,
		Event:     make(map[string]QueueListenerHealth, len(c.healthSnapshot.Event)),
		Delay:     make(map[string]QueueListenerHealth, len(c.healthSnapshot.Delay)),
		Timer:     c.healthSnapshot.Timer,
	}
	for k, v := range c.healthSnapshot.Event {
		cp.Event[k] = v
	}
	for k, v := range c.healthSnapshot.Delay {
		cp.Delay[k] = v
	}
	return cp
}

// HealthOK 返回监听链路是否健康。
func (c *Consumer) HealthOK() bool {
	snap := c.HealthSnapshot()
	if !snap.Running {
		return false
	}

	eventEnabled := c.cfg.EventDriver != nil && c.eventFactory != nil
	delayEnabled := c.cfg.DelayDriver != nil && c.delayFactory != nil
	timerEnabled := c.cfg.LockDriver != nil && c.timerFactory != nil && len(c.registry.GetTimerTasks()) > 0

	if eventEnabled {
		for _, st := range snap.Event {
			if !st.Alive || st.LenError != "" {
				return false
			}
		}
	}
	if delayEnabled {
		for _, st := range snap.Delay {
			if !st.Alive || st.LenError != "" {
				return false
			}
		}
	}
	if timerEnabled && !snap.Timer.Alive {
		return false
	}

	return true
}

func (c *Consumer) recordHeartbeat(hb core.ListenerHeartbeat) {
	beatAt := hb.At
	if beatAt.IsZero() {
		beatAt = time.Now()
	}

	c.healthMu.Lock()
	defer c.healthMu.Unlock()
	switch hb.Kind {
	case core.ListenerKindEvent:
		c.eventBeatAt[hb.Name] = beatAt
	case core.ListenerKindDelay:
		c.delayBeatAt[hb.Name] = beatAt
	case core.ListenerKindTimer:
		c.timerBeatAt = beatAt
	}
}

func (c *Consumer) startMonitorLocked() {
	ctx, cancel := context.WithCancel(context.Background())
	c.monitorCancel = cancel
	c.monitorWG.Add(1)
	go func() {
		defer c.monitorWG.Done()
		ticker := time.NewTicker(c.cfg.HealthInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.refreshHealthSnapshot(ctx, true)
			}
		}
	}()
}

func (c *Consumer) refreshHealthSnapshot(ctx context.Context, running bool) {
	now := time.Now()
	eventEntries := c.registry.GetEventRunners()
	delayEntries := c.registry.GetDelayRunners()

	c.healthMu.RLock()
	eventBeat := make(map[string]time.Time, len(c.eventBeatAt))
	delayBeat := make(map[string]time.Time, len(c.delayBeatAt))
	for k, v := range c.eventBeatAt {
		eventBeat[k] = v
	}
	for k, v := range c.delayBeatAt {
		delayBeat[k] = v
	}
	timerBeat := c.timerBeatAt
	c.healthMu.RUnlock()

	snap := HealthSnapshot{
		Running:   running,
		CheckedAt: now,
		Event:     make(map[string]QueueListenerHealth, len(eventEntries)),
		Delay:     make(map[string]QueueListenerHealth, len(delayEntries)),
	}

	eventGroups := c.groupEventEntries(eventEntries)
	for groupName := range eventGroups {
		keys := queue.NewQueueKeySet(c.cfg.KeyPrefix, "event", groupName)
		item := QueueListenerHealth{LastBeatAt: eventBeat[groupName]}
		item.Alive = !item.LastBeatAt.IsZero() && now.Sub(item.LastBeatAt) <= c.cfg.HealthBeatTimeout
		if c.cfg.EventDriver != nil {
			lenCtx, cancel := c.internalOpContext(ctx, defaults.HealthLenTimeout)
			n, err := c.cfg.EventDriver.Len(lenCtx, keys.Pending)
			cancel()
			if err != nil {
				item.LenError = err.Error()
			} else {
				item.PendingLen = n
			}
		}
		snap.Event[groupName] = item
	}

	for name := range delayEntries {
		keys := queue.NewQueueKeySet(c.cfg.KeyPrefix, "delay", name)
		item := QueueListenerHealth{LastBeatAt: delayBeat[name]}
		item.Alive = !item.LastBeatAt.IsZero() && now.Sub(item.LastBeatAt) <= c.cfg.HealthBeatTimeout
		if c.cfg.DelayDriver != nil {
			lenCtx, cancel := c.internalOpContext(ctx, defaults.HealthLenTimeout)
			n, err := c.cfg.DelayDriver.Len(lenCtx, keys.Pending)
			cancel()
			if err != nil {
				item.LenError = err.Error()
			} else {
				item.PendingLen = n
			}
		}
		snap.Delay[name] = item
	}

	snap.Timer = TimerListenerHealth{
		LastBeatAt: timerBeat,
		Alive:      !timerBeat.IsZero() && now.Sub(timerBeat) <= c.cfg.HealthBeatTimeout,
	}

	c.healthMu.Lock()
	c.healthSnapshot = snap
	c.healthMu.Unlock()

	if running {
		c.checkHealthAlerts(snap)
	}
}

func (c *Consumer) checkHealthAlerts(snap HealthSnapshot) {
	threshold := c.cfg.HealthAlertThreshold
	if threshold <= 0 {
		return
	}

	c.healthMu.Lock()
	defer c.healthMu.Unlock()

	for name, st := range snap.Event {
		key := "event:" + name
		if !st.Alive || st.LenError != "" {
			c.healthFailCounts[key]++
			if c.healthFailCounts[key] == threshold {
				reason := "heartbeat timeout"
				if st.LenError != "" {
					reason = "pending len error: " + st.LenError
				}
				c.cfg.Logger.Errorf("taskx: event[%s] unhealthy for %d consecutive checks: %s", name, threshold, reason)
				c.enqueueAlert(
					core.AlertData{
						Source:     core.AlertSourceEvent,
						AlertType:  core.AlertListenerUnhealthy,
						RunnerName: name,
						Remark:     fmt.Sprintf("consecutive failures: %d, reason: %s", threshold, reason),
					},
				)
			}
		} else {
			if c.healthFailCounts[key] > 0 {
				c.cfg.Logger.Infof("taskx: event[%s] recovered after %d consecutive failures", name, c.healthFailCounts[key])
			}
			c.healthFailCounts[key] = 0
		}
	}

	for name, st := range snap.Delay {
		key := "delay:" + name
		if !st.Alive || st.LenError != "" {
			c.healthFailCounts[key]++
			if c.healthFailCounts[key] == threshold {
				reason := "heartbeat timeout"
				if st.LenError != "" {
					reason = "pending len error: " + st.LenError
				}
				c.cfg.Logger.Errorf("taskx: delay[%s] unhealthy for %d consecutive checks: %s", name, threshold, reason)
				c.enqueueAlert(
					core.AlertData{
						Source:     core.AlertSourceDelay,
						AlertType:  core.AlertListenerUnhealthy,
						RunnerName: name,
						Remark:     fmt.Sprintf("consecutive failures: %d, reason: %s", threshold, reason),
					},
				)
			}
		} else {
			if c.healthFailCounts[key] > 0 {
				c.cfg.Logger.Infof("taskx: delay[%s] recovered after %d consecutive failures", name, c.healthFailCounts[key])
			}
			c.healthFailCounts[key] = 0
		}
	}

	timerKey := "timer"
	if !snap.Timer.Alive {
		c.healthFailCounts[timerKey]++
		if c.healthFailCounts[timerKey] == threshold {
			c.cfg.Logger.Errorf("taskx: timer unhealthy for %d consecutive checks: heartbeat timeout", threshold)
			c.enqueueAlert(
				core.AlertData{
					Source:    core.AlertSourceTimer,
					AlertType: core.AlertListenerUnhealthy,
					Remark:    fmt.Sprintf("consecutive failures: %d, reason: heartbeat timeout", threshold),
				},
			)
		}
	} else {
		if c.healthFailCounts[timerKey] > 0 {
			c.cfg.Logger.Infof("taskx: timer recovered after %d consecutive failures", c.healthFailCounts[timerKey])
		}
		c.healthFailCounts[timerKey] = 0
	}
}
