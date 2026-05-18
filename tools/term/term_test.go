package term

import (
	"fmt"
	"syscall"
	"testing"
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
)

func TestMain(m *testing.M) {
	m.Run()
}

func TestGetTerminateReceiver(t *testing.T) {
	// one := GetTerminateReceiver()
}

func TestAddFunc(t *testing.T) {
	one := GetTerminateReceiver()

	testFunc := func() {
		fmt.Println("exec testFunc")
	}
	one.AddDefaultHandler(testFunc)

	fmt.Println("start", time.Now())
	go func() {
		time.Sleep(10 * time.Second)
		// 模拟发送退出信号
		syscall.Kill(syscall.Getpid(), syscall.SIGINT)
	}()

	// one.SyncWatch()
	one.Watch().Wait()

	fmt.Println("end  ", time.Now())
}

func TestTrerm(t *testing.T) {
	l, err := logger_factory.NewExample()
	if err != nil {
		panic(err)
	}

	// graceful quit
	GetTerminateReceiver().AddDefaultHandler(
		func() {
			time.Sleep(200 * time.Millisecond)
			fmt.Println("exit func")
		},
	).SetLogger(l).SyncWatch()
}

func TestQuit(t *testing.T) {
	l, err := logger_factory.NewExample()
	if err != nil {
		panic(err)
	}

	ti := time.NewTimer(1000 * time.Millisecond)

	go func() {
		select {
		case <-ti.C:
			VoluntaryWithdrawal()
			fmt.Println("timer expired")
		}
	}()

	t.Log("start")

	// graceful quit
	GetTerminateReceiver().AddDefaultHandler(
		func() {
			time.Sleep(200 * time.Millisecond)
			fmt.Println("exit func")
		},
	).SetLogger(l).SyncWatch()

	if !IsStop() {
		t.Error("IsStop() failed")
	}
}
