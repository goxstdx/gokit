package connections

import (
	"context"

	"gorm.io/gorm"

	components_mysql "gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/mysql"
)

var (
	mysqlClient    *gorm.DB
	mysqlClientMap map[string]*gorm.DB
)

// GenDefaultMysqlClient 注意，这里每次执行都会覆盖默认的链接
func GenDefaultMysqlClient(ctx context.Context, c *components_mysql.Options) (err error) {
	mysqlClient, err = components_mysql.InitMysqlClient(ctx, c)
	if err != nil {
		return err
	}

	return nil
}

// GetMysqlClient 获取默认的 mysql 链接
func GetMysqlClient() *gorm.DB {
	return mysqlClient
}

// GenDefaultManyMysqlClient 注意，这里每次执行都会覆盖默认的多实例链接
func GenDefaultManyMysqlClient(ctx context.Context, c *components_mysql.Options) (err error) {
	mysqlClientMap, err = components_mysql.InitManyMysqlClient(ctx, c)
	if err != nil {
		return err
	}

	return nil
}

// GetManyMysqlClient 获取默认的多实例 mysql 链接
func GetManyMysqlClient() map[string]*gorm.DB {
	return mysqlClientMap
}

// GetMysqlClientByName 按名称获取多实例 mysql 链接
func GetMysqlClientByName(name string) *gorm.DB {
	if mysqlClientMap == nil {
		return nil
	}
	return mysqlClientMap[name]
}
