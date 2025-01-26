package rabbitmq

import (
	"crypto/tls"
	"errors"
	"fmt"
	"log"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type MQ struct {
	Name     string `toml:"name"`
	User     string `toml:"user"`
	Password string `toml:"password"`
	Addr     string `toml:"addr"`
	Port     int    `toml:"port"`
	Vhost    string `toml:"vhost"`
}

func (r MQ) uri() string {
	return fmt.Sprintf("amqps://%s:%s@%s:%d/%s", r.User, r.Password, r.Addr, r.Port, r.Vhost)
}

const (
	reconnectDelay   = 5 * time.Second // 连接断开后多久重连
	reconnectMaxTime = 3               // 发送消息或订阅时，等待重连次数
	resendDelay      = 5 * time.Second // 消息发送失败后，多久重发
	resendTime       = 3               // 消息重发次数
)

type RabbitMQ struct {
	ConnectUri string // 连接地址

	IsConnected bool             // 是否已连接
	Done        chan bool        // 正常关闭
	NotifyClose chan *amqp.Error // 异常关闭

	connection *amqp.Connection // 连接
	channel    *amqp.Channel    // 通道
}

func NewRabbitMQClient(cfg MQ) (*RabbitMQ, error) {
	mq := &RabbitMQ{
		ConnectUri: cfg.uri(),
	}

	if err := mq.conn(); err != nil {
		return nil, err
	}
	return mq, nil
}

func (r *RabbitMQ) GetChannel() *amqp.Channel {
	return r.channel
}

func (r *RabbitMQ) GetConn() *amqp.Connection {
	return r.connection
}

// 创建链接
func (r *RabbitMQ) conn() error {
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true, // 忽略证书验证
	}

	conn, err := amqp.DialTLS(r.ConnectUri, tlsConfig)
	if err != nil {
		return fmt.Errorf("无法连接到RabbitMQ服务器: %s", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("无法打开RabbitMQ通道: %s", err)
	}

	r.connection = conn
	r.channel = ch
	r.IsConnected = true
	r.Done = make(chan bool)

	r.NotifyClose = make(chan *amqp.Error)
	r.channel.NotifyClose(r.NotifyClose)

	// 开启持续重连监测
	go r.monitorConn()

	return nil
}

// 关闭链接
func (r *RabbitMQ) CloseConn() {

	if r != nil && r.IsConnected {

		close(r.Done)

		if r.channel != nil {
			r.channel.Close()
		}

		if r.connection != nil {
			r.connection.Close()
		}

		r.IsConnected = false
	}
}

// 发送数据或订阅时候 等待重连
func (r *RabbitMQ) WaitConn() error {
	i := 1
	for {
		if i >= reconnectMaxTime {
			goto END
		}
		if r.IsConnected {
			goto END
		}
		i++
		time.Sleep(reconnectDelay)
	}

END:
	if r.IsConnected {
		return nil
	}
	return errors.New("connect rabbitMQ fail")
}

// 持续监测链接
func (r *RabbitMQ) monitorConn() {
	for {
		if !r.IsConnected {
			log.Println("[rabbitMQ]", "connect rabbitMQ")
			if err := r.conn(); err != nil {
				log.Println("[rabbitMQ]", "Failed to connect rabbitMQ. Retrying...")
			}
		}

		select {
		case <-r.Done:
			return
		case <-r.NotifyClose:
			if r.IsConnected {
				if r.channel != nil {
					r.channel.Close()
				}
				if r.connection != nil {
					r.connection.Close()
				}
				r.IsConnected = false
			}
		}
		time.Sleep(reconnectDelay)
	}
}
