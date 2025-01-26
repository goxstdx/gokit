package components_mysql

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

// InitManyMysqlClient
/*
 *  初始化 mysql 连接
 *
 *  @param ctx context.Context
 *  @param c map[string]MysqlClientConf
 *
 *  @Author: zhouxing@sailone.team
 *  @Date: 2023-07-18 17:49:41
 */
func InitManyMysqlClient(ctx context.Context, c *Options) (map[string]*gorm.DB, error) {
	cfg = c
	if cfg == nil || cfg.ManyClientConf == nil || cfg.Logger == nil {
		return nil, fmt.Errorf("mysql client not initialized")
	}

	// 验证配置
	err := checkConf()
	if err != nil {
		return nil, err
	}
	cfg.Logger.Infof("InitManyMysqlClient cfg: %+v", cfg)

	defer func() {
		// 这个包不管理连接
		clientMap = nil
	}()

	// 初始化
	return clientMap, initMysql()
}

// InitMysqlClient
/*
 *  初始化 mysql 连接
 *
 *  @param ctx context.Context
 *  @param c map[string]MysqlClientConf
 *
 *  @Author: zhouxing@sailone.team
 *  @Date: 2023-07-18 17:49:41
 */
func InitMysqlClient(ctx context.Context, c *Options) (*gorm.DB, error) {
	if c == nil || c.ClientConf == nil || c.Logger == nil {
		return nil, fmt.Errorf("mysql client not initialized")
	}

	c.ManyClientConf = make(map[string]MysqlClientConf)
	c.ManyClientConf["default"] = *c.ClientConf

	cfg = c
	if cfg == nil || cfg.ManyClientConf == nil || cfg.Logger == nil {
		return nil, fmt.Errorf("mysql client not initialized")
	}

	// 验证配置
	err := checkConf()
	if err != nil {
		return nil, err
	}
	cfg.Logger.Infof("InitManyMysqlClient cfg: %+v", cfg)

	defer func() {
		// 这个包不管理连接
		clientMap = nil
	}()

	// 初始化
	return clientMap["default"], initMysql()
}
