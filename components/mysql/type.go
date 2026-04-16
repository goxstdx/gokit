package components_mysql

import (
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
)

const (
	// 用于设置连接池中空闲连接的最大数量。
	DEFAULT_MAX_IDLE_CONNS int = 2
	// 设置打开数据库连接的最大数量。
	DEFAULT_MAX_OPEN_CONNS int = 25
	// 设置了连接可复用的最大时间。
	DEFAULT_CONN_MAX_LIFETIME time.Duration = time.Hour * 1
	DEFAULT_DB_CHARSET                      = "utf8mb4"
	DEFAULT_DB_PORT                         = "3306"
)

type Options struct {
	ManyClientConf map[string]MysqlClientConf

	ClientConf *MysqlClientConf

	Logger logger_factory.Logger
}

type MysqlClientConf struct {
	// 注册时的名字，直接使用key
	// ClientName string
	Host      string
	Port      string
	User      string
	Password  string
	DbName    string
	DbCharset string

	PoolConf *MysqlClientPoolConf

	Dsn string

	Logger          logger.Interface
	CreateBatchSize int // 执行批量操作时，一次最大的数量，比如批量插入
}

type MysqlClientPoolConf struct {
	// 用于设置连接池中空闲连接的最大数量。
	MaxIdleConns int
	// 设置打开数据库连接的最大数量。
	MaxOpenConns int
	// 设置了连接可复用的最大时间。
	ConnMaxLifetime time.Duration
}

var cfg *Options

var clientMap map[string]*gorm.DB
