package tools

import (
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"
)

func FullStack() string {
	var buf [2 << 11]byte
	runtime.Stack(buf[:], true)
	return Bytes2Str(buf[:])
}

func WaitForShutdownSignal(fn func(), sig ...os.Signal) {
	c := make(chan os.Signal, 1)

	sig = append(sig, syscall.SIGINT, syscall.SIGTERM)
	signal.Notify(c, sig...)
	<-c

	fmt.Println("Received shutdown signal, exiting...")
	fn()
}
