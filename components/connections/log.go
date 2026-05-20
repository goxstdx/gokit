package connections

import (
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
)

var (
	logger logger_factory.Logger
)

// GenDefaultLogger 注意，这里每次执行都会覆盖默认的 logger
func GenDefaultLogger(cfg logger_factory.Config) (err error) {
	logger, err = logger_factory.NewLogger(cfg)
	if err != nil {
		return err
	}

	return nil
}

// GetLogger 获取默认的 logger
func GetLogger() logger_factory.Logger {
	return logger
}
