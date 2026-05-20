package connections

import (
	"gitlab.ops.gooddriver.io/mutual_public/go-mutual-common/components/rabbitmq"
)

var (
	rabbitmqClient *rabbitmq.RabbitMQ
)

// GenDefaultRabbitMQClient 注意，这里每次执行都会覆盖默认的链接
func GenDefaultRabbitMQClient(cfg rabbitmq.MQ) (err error) {
	rabbitmqClient, err = rabbitmq.NewRabbitMQClient(cfg)
	if err != nil {
		return err
	}

	return nil
}

// GetRabbitMQClient 获取默认的 rabbitmq 链接
func GetRabbitMQClient() *rabbitmq.RabbitMQ {
	return rabbitmqClient
}
