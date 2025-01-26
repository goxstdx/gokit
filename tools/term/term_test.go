package term

import (
	"fmt"
	"syscall"
	"testing"
	"time"

	"go.uber.org/zap"
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

	//one.SyncWatch()
	one.Watch().Wait()

	fmt.Println("end  ", time.Now())
}

func TestTrerm(t *testing.T) {
	// graceful quit
	GetTerminateReceiver().AddDefaultHandler(func() {
		time.Sleep(200 * time.Millisecond)
		fmt.Println("exit func")
	}).SetLogger(zap.NewExample()).SyncWatch()
}
