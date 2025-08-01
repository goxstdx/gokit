package components_mysql

import (
	"time"

	"gorm.io/gorm/logger"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/zap_log"
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
