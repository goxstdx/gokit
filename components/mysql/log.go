package components_mysql

import (
	"time"

	"gorm.io/gorm/logger"
)

func newSqlLog() logger.Interface {
	return cfg.Logger.GetGormLogger(
		logger.Config{
			SlowThreshold:             2 * time.Second, // 慢查询时间
			Colorful:                  false,
			IgnoreRecordNotFoundError: false, // 抛弃未找到错误
			ParameterizedQueries:      true,  // 不显示完整的 SQL 查询参数，使用 ? 占位符
			LogLevel:                  logger.Info,
		},
	)
}
