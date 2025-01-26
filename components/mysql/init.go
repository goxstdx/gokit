package components_mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"sync"

	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func checkConf() (err error) {
	for key, conf := range cfg.ManyClientConf {
		// 配置初始化时就已经拼好了
		if conf.Dsn == "" {
			if conf.Host == "" {
				return errors.New(fmt.Sprintf("the host of `%s` is required", key))
			}
			if conf.Port == "" {
				conf.Port = DEFAULT_DB_PORT
			}
			if conf.DbCharset == "" {
				conf.DbCharset = DEFAULT_DB_CHARSET
			}
			conf.Dsn = fmt.Sprintf("%v:%v@tcp(%v:%v)/%v?charset=%v&parseTime=true",
				conf.User, conf.Password, conf.Host, conf.Port, conf.DbName, conf.DbCharset)
		}

		if conf.PoolConf == nil {
			conf.PoolConf = &MysqlClientPoolConf{
				MaxIdleConns:    DEFAULT_MAX_IDLE_CONNS,
				MaxOpenConns:    DEFAULT_MAX_OPEN_CONNS,
				ConnMaxLifetime: DEFAULT_CONN_MAX_LIFETIME,
			}
		}
		if conf.PoolConf.MaxIdleConns == 0 {
			conf.PoolConf.MaxIdleConns = DEFAULT_MAX_IDLE_CONNS
		}
		if conf.PoolConf.MaxOpenConns == 0 {
			conf.PoolConf.MaxOpenConns = DEFAULT_MAX_OPEN_CONNS
		}
		if conf.PoolConf.ConnMaxLifetime == 0 {
			conf.PoolConf.ConnMaxLifetime = DEFAULT_CONN_MAX_LIFETIME
		}

		if conf.Logger == nil {
			conf.Logger = newSqlLog()
		}

		cfg.ManyClientConf[key] = conf
	}

	return nil
}

func initMysql() error {
	clientMap = make(map[string]*gorm.DB, len(cfg.ManyClientConf))

	for clientName, conf := range cfg.ManyClientConf {
		err := initOneMysql(clientName, conf)
		if err != nil {
			cfg.Logger.Errorf("initMysql clientName: %v is FAIL, err: %v", clientName, err)

			return err
		}
		cfg.Logger.Infof("initMysql clientName: %v is succ", clientName)
	}

	return nil
}

var initLock sync.Mutex

func initOneMysql(clientName string, conf MysqlClientConf) error {
	// 提前加锁，避免重复初始化
	initLock.Lock()
	defer initLock.Unlock()

	if db, ok := clientMap[clientName]; ok {
		_, sqlErr := checkClientStatus(db)
		// 连接已经存在，且可用，不重新创建
		if sqlErr == nil {
			return nil
		}
	}

	db, dbErr := gorm.Open(mysql.New(mysql.Config{
		DSN:                      conf.Dsn,
		DefaultStringSize:        256,  // string 类型字段默认长度
		DisableDatetimePrecision: true, // 禁用datatime精度,5.6版本之前不支持
	}), &gorm.Config{
		Logger:          conf.Logger,
		QueryFields:     true, // 表名查询字段而非 select *
		CreateBatchSize: conf.CreateBatchSize,
	})
	if dbErr != nil {
		return dbErr
	}

	// 获取通用数据库对象 sql.DB ，然后使用其提供的功能
	sqlDB, sqlErr := checkClientStatus(db)
	if sqlErr != nil {
		return sqlErr
	}

	// SetMaxIdleConns 用于设置连接池中空闲连接的最大数量。
	sqlDB.SetMaxIdleConns(conf.PoolConf.MaxIdleConns)
	// SetMaxOpenConns 设置打开数据库连接的最大数量。
	sqlDB.SetMaxOpenConns(conf.PoolConf.MaxOpenConns)
	// SetConnMaxLifetime 设置了连接可复用的最大时间。
	sqlDB.SetConnMaxLifetime(conf.PoolConf.ConnMaxLifetime)

	// 添加到连接 map
	clientMap[clientName] = db

	return nil
}

// 检测连接状态，如果连接异常，会主动关闭连接
func checkClientStatus(db *gorm.DB) (sqlDB *sql.DB, err error) {
	// 获取通用数据库对象 sql.DB ，然后使用其提供的功能
	sqlDB, err = db.DB()
	if err != nil {
		return sqlDB, err
	}
	if err = sqlDB.Ping(); err != nil {
		return sqlDB, err
	}

	return sqlDB, nil
}
