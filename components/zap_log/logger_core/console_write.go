package logger_core

import "os"

func getConsoleWrite() *writerWrapper {
	return &writerWrapper{writer: os.Stdout}
}

// writerWrapper 是一个简单的包装器，用于实现 zapcore.WriteSyncer 接口
type writerWrapper struct {
	writer *os.File
}

func (w *writerWrapper) Write(p []byte) (n int, err error) {
	return w.writer.Write(p)
}

// Sync 要重写一下，不然一直报错
func (w *writerWrapper) Sync() error {
	return nil // 对于标准输出，通常不需要同步操作
}
