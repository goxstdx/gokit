package components_mysql

import (
	"time"

	"gitlab.ops.haochezhu.club/mutuals/go-mutual-common/components/zap_log"
	"gorm.io/gorm/logger"
)

func newSqlLog() logger.Interface {
	return zap_log.NewGormLogger(cfg.Logger.Desugar(), logger.Config{
		SlowThreshold:             2 * time.Second, // 慢查询时间
		Colorful:                  false,
		IgnoreRecordNotFoundError: false, // 抛弃未找到错误
		ParameterizedQueries:      true,  // 不显示完整的 SQL 查询参数，使用 ? 占位符
		LogLevel:                  logger.Info,
	})
}
