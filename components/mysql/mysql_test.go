package components_mysql

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/logger_factory"
)

// VehicleAiMatchResult Ai车型库匹配结果
type VehicleAiMatchResult struct {
	ID            int64     `json:"id" gorm:"id"`                           // 主键
	QuoteNo       string    `json:"quote_no" gorm:"quote_no"`               // 报价申请记录单号
	FormVehicleId string    `json:"form_vehicle_id" gorm:"form_vehicle_id"` // 车辆唯一id
	CompanyName   string    `json:"company_name" gorm:"company_name"`       // 保司名字
	State         string    `json:"state" gorm:"state"`                     // 所在洲简写，例如：CA
	MatchType     int16     `json:"match_type" gorm:"match_type"`           // 匹配类型，1：ai返回结果中提取，2：兜底逻辑匹配
	TaskId        string    `json:"task_id" gorm:"task_id"`                 // 请求唯一id，correlationId字段
	OriginData    string    `json:"origin_data" gorm:"origin_data"`         // 匹配到的元数据
	Ctime         time.Time `json:"ctime" gorm:"ctime"`                     // 创建时间
	Mtime         time.Time `json:"mtime" gorm:"mtime"`                     // 更新时间
}

// TableName 表名称
func (*VehicleAiMatchResult) TableName() string {
	return "vehicle_ai_match_result"
}

func TestManyMysql(t *testing.T) {
	ctx := context.Background()

	logger, err := logger_factory.NewExample()
	if err != nil {
		panic(err)
	}

	conf := &Options{
		ManyClientConf: map[string]MysqlClientConf{
			"mutual_join": {
				// Host:     "110.238.75.32",
				// Port:     "3306",
				// User:     "work",
				// Password: "work@dev",
				// DbName:   "microloan",
				Dsn: "root:@tcp(127.0.0.1:3306)/mutual_join?charset=utf8mb4&parseTime=true",
				PoolConf: &MysqlClientPoolConf{
					MaxIdleConns:    1,
					MaxOpenConns:    0,
					ConnMaxLifetime: 10,
				},
			},
			"mutual_pay": {
				Host:     "127.0.0.1",
				Port:     "3306",
				User:     "root",
				Password: "",
				DbName:   "mutual_pay",
			},
		},
		Logger: logger,
	}
	dbMap, _ := InitManyMysqlClient(ctx, conf)

	client, ok := dbMap["mutual_join"]
	if !ok {
		fmt.Println("GetClientByClientName not exists")
	}

	m := map[string]interface{}{}
	err = client.Table("vehicle_ai_match_record").Find(&m).Error
	fmt.Println("Find err", err)

	fmt.Println(fmt.Sprintf("m: %+v", m))

	client2, ok := dbMap["mutual_join"]
	if !ok {
		fmt.Println("GetClientByClientName not exists")
	}

	m2 := VehicleAiMatchResult{}
	err = client2.Table(m2.TableName()).First(&m2).Error
	fmt.Println("Find err", err)

	fmt.Println(fmt.Sprintf("m2: %+v", m))
}

func TestOneMysql(t *testing.T) {
	ctx := context.Background()

	logger, err := logger_factory.NewExample()
	if err != nil {
		panic(err)
	}

	conf := &Options{
		ClientConf: &MysqlClientConf{
			Host:     "127.0.0.1",
			Port:     "3306",
			User:     "root",
			Password: "",
			DbName:   "haochezhu",
		},
		Logger: logger,
	}
	client, err := InitMysqlClient(ctx, conf)

	if err != nil {
		fmt.Println("GetClientByClientName not exists")
	}

	m := map[string]interface{}{}
	err = client.Table("vehicle_ai_match_record").Find(&m).Error
	fmt.Println("Find err", err)

	fmt.Println(fmt.Sprintf("m: %+v", m))

	m2 := VehicleAiMatchResult{}
	err = client.Table(m2.TableName()).First(&m2).Error
	fmt.Println("Find err", err)

	fmt.Println(fmt.Sprintf("m2: %+v", m2))
}
