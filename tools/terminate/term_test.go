package terminate

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestGetTerminateReceiver(t *testing.T) {
	first := GetTerminateReceiver()
	second := GetTerminateReceiver()

	if first != second {
		t.Fatal("GetTerminateReceiver() should return the same singleton instance")
	}
}

func TestTriggerShutdownTriggersHandler(t *testing.T) {
	receiver := NewTerminateReceiver()

	handlerCalled := make(chan struct{}, 1)
	receiver.AddDefaultHandler(
		func() {
			handlerCalled <- struct{}{}
		},
	)

	receiver.Watch()
	receiver.TriggerShutdown()
	receiver.Wait()

	select {
	case <-handlerCalled:
	default:
		t.Fatal("default handler was not called")
	}

	if !receiver.IsStop() {
		t.Fatal("receiver should be stopped after voluntary withdrawal")
	}
}

func TestSyncWatchWithTriggerShutdown(t *testing.T) {
	receiver := NewTerminateReceiver()

	var called atomic.Int32
	receiver.AddDefaultHandler(
		func() {
			called.Add(1)
		},
	)

	go func() {
		time.Sleep(10 * time.Millisecond)
		receiver.TriggerShutdown()
	}()

	receiver.SyncWatch()

	if called.Load() != 1 {
		t.Fatalf("default handler called %d times, want 1", called.Load())
	}

	if !receiver.IsStop() {
		t.Fatal("receiver should be stopped after SyncWatch returns")
	}
}

func TestDuplicateWatchIgnored(t *testing.T) {
	receiver := NewTerminateReceiver()

	var called atomic.Int32
	receiver.AddDefaultHandler(
		func() {
			called.Add(1)
		},
	)

	receiver.Watch()
	receiver.Watch()
	receiver.TriggerShutdown()
	receiver.Wait()

	if called.Load() != 1 {
		t.Fatalf("default handler called %d times, want 1", called.Load())
	}
}

func TestTriggerShutdownWithoutDefaultHandler(t *testing.T) {
	receiver := NewTerminateReceiver()

	receiver.Watch()
	receiver.TriggerShutdown()
	receiver.Wait()

	if !receiver.IsStop() {
		t.Fatal("receiver should be stopped even when default handler is nil")
	}
}
