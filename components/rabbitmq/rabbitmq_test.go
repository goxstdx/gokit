package rabbitmq

import (
	"context"
	"fmt"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// name = "pay"
// user = "pay.test"
// password = "VPu8GN8hfsZGndMY"
// addr = "192.168.96.247"
// port = 15671
// vhost = "pay.test"

func TestSendMessage(t *testing.T) {
	mq, err := NewRabbitMQClient(MQ{
		Name:     "pay",
		User:     "pay.test",
		Password: "VPu8GN8hfsZGndMY",
		Addr:     "192.168.96.247",
		Port:     15671,
		Vhost:    "pay.test",
	})
	if err != nil {
		panic(err)
	}

	q, err := mq.GetChannel().QueueDeclare(
		"zx_test", // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := "Hello World!"
	err = mq.GetChannel().PublishWithContext(ctx,
		"",     // exchange
		q.Name, // routing key
		false,  // mandatory
		false,  // immediate
		amqp.Publishing{
			ContentType: "text/plain",
			Body:        []byte(body),
		})
	if err != nil {
		panic(err)
	}
	fmt.Println(fmt.Sprintf(" [x] Sent %s\n", body))
}

func TestReceive(t *testing.T) {
	mq, err := NewRabbitMQClient(MQ{
		Name:     "pay",
		User:     "pay.test",
		Password: "VPu8GN8hfsZGndMY",
		Addr:     "192.168.96.247",
		Port:     15671,
		Vhost:    "pay.test",
	})
	if err != nil {
		panic(err)
	}
	q, err := mq.GetChannel().QueueDeclare(
		"zx_test", // name
		false,     // durable
		false,     // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		panic(err)
	}

	msgs, err := mq.GetChannel().Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		panic(err)
	}

	forever := make(chan struct{})

	go func() {
		for d := range msgs {
			fmt.Println(fmt.Sprintf("Received a message: %s", d.Body))
		}

		forever <- struct{}{}
	}()

	fmt.Println(fmt.Sprintf(" [*] Waiting for messages. To exit press CTRL+C"))
	<-forever
}
