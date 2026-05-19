package consumer

import (
	"context"
	"fmt"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/taskx/internal/core"
)

func (c *Consumer) startAlertDispatcherLocked() {
	c.alertHandler = c.cfg.OnAlert
	c.alertQueue = make(chan core.AlertData, c.cfg.AlertQueueSize)

	ctx, cancel := context.WithCancel(context.Background())
	c.alertCancel = cancel
	c.alertWG.Add(1)
	go func() {
		defer c.alertWG.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case data := <-c.alertQueue:
				if c.alertHandler != nil {
					c.alertHandler(data)
				}
			}
		}
	}()
}

func (c *Consumer) stopAlertDispatcherLocked() {
	c.shutdownAlertDispatcher()
}

func (c *Consumer) stopAlertDispatcherWithContextLocked(ctx context.Context) error {
	if c.alertCancel != nil {
		c.alertCancel()
		c.alertCancel = nil
	}
	if err := waitWithContext(ctx, c.alertWG.Wait); err != nil {
		return fmt.Errorf("taskx: wait alert dispatcher stop: %w", err)
	}
	c.drainAndRestoreAlert()
	return nil
}

func (c *Consumer) shutdownAlertDispatcher() {
	if c.alertCancel != nil {
		c.alertCancel()
		c.alertCancel = nil
	}
	c.alertWG.Wait()
	c.drainAndRestoreAlert()
}

func (c *Consumer) drainAndRestoreAlert() {
	if c.alertQueue != nil {
		drained := 0
		for {
			select {
			case data := <-c.alertQueue:
				drained++
				if c.cfg.Logger != nil {
					c.cfg.Logger.Warnf(
						"taskx: alert queue drain on stop, source=%s type=%s runner=%s envelope_id=%s",
						data.Source, data.AlertType, data.RunnerName, alertEnvelopeID(data.Envelope),
					)
				}
			default:
				if drained > 0 && c.cfg.Logger != nil {
					c.cfg.Logger.Warnf("taskx: drained %d pending alerts on stop", drained)
				}
				c.alertQueue = nil
				break
			}
			if c.alertQueue == nil {
				break
			}
		}
	}
	c.alertHandler = nil
}

func (c *Consumer) enqueueAlert(data core.AlertData) {
	if data.Source == "" && data.Envelope != nil {
		switch data.Envelope.Source {
		case core.EnvelopeSourceEvent:
			data.Source = core.AlertSourceEvent
		case core.EnvelopeSourceDelay:
			data.Source = core.AlertSourceDelay
		case core.EnvelopeSourceTimer:
			data.Source = core.AlertSourceTimer
		}
	}

	if c.alertQueue == nil {
		if c.alertHandler != nil {
			c.alertHandler(data)
		}
		return
	}

	select {
	case c.alertQueue <- data:
	default:
		c.cfg.Logger.Warnf(
			"taskx: alert queue full, dropping alert source=%s type=%s runner=%s envelope_id=%s",
			data.Source, data.AlertType, data.RunnerName, alertEnvelopeID(data.Envelope),
		)
	}
}

func alertEnvelopeID(env *core.Envelope) string {
	if env == nil {
		return ""
	}
	return env.ID
}
