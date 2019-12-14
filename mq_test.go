package rabbitmq

import (
	"github.com/labstack/gommon/log"
	"github.com/streadway/amqp"
	"testing"
	"time"
)

const (
	connectRabbitMQFailed  = "Failed to connect to RabbitMQ"
	openChannelFailed      = "Failed to open a channel"
	declareQueueFailed     = "Failed to declare a queue"
	bindQueueFailed        = "Failed to bind a queue"
	declareExchangeFailed  = "Failed to declare an exchange"
	registerConsumerFailed = "Failed to register a consumer"
	publishMessageFailed   = "Failed to publish a message"
	setQoSFailed           = "Failed to set Qos"
	connectRabbitMQSucceed = "RabbitMQ connected!"
)

var url = "amqp://ricnsmart:9ef16689fdaf@dev.ricnsmart.com:5672/"

func TestPublish(t *testing.T) {
	c := New(url)
	if err := c.Open(); err != nil {
		log.Fatal(err)
	}
	forever := make(chan byte)
	go func() {
		for {
			obj, err := c.Channel()
			if err != nil {
				log.Error("获取Channel失败！", err)
				continue
			}
			ch := obj.(*amqp.Channel)
			_, err = ch.QueueDeclare(
				"test",
				false,
				true,
				false,
				false,
				nil,
			)
			if err != nil {
				log.Errorf(`%v,%v`, declareQueueFailed, err)
				continue
			}
			for i := 0; i < 10; i++ {
				err = ch.Publish(
					"",
					"test",
					false,
					false,
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte{1, 2, 3},
					})
				if err != nil {
					log.Errorf(`%v,%v`, publishMessageFailed, err)
					break
				}
				time.Sleep(2 * time.Second)
			}
			err = c.ReturnChannel(obj)
			if err != nil {
				log.Error(err)
				continue
			}
		}
	}()

	go func() {
		for i := 0; i < 3; i++ {
			log.Infof("第%v次重试", i)
			obj, err := c.Channel()
			if err != nil {
				log.Error("获取Channel失败！", err)
				continue
			}
			ch := obj.(*amqp.Channel)
			q, err := ch.QueueDeclare(
				"test",
				false,
				true,
				false,
				false,
				nil,
			)
			if err != nil {
				log.Errorf(`%v,%v`, declareQueueFailed, err)
				continue
			}
			for i := 0; i < 100; i++ {
				err = ch.Publish(
					"",
					q.Name,
					false,
					false,
					amqp.Publishing{
						ContentType: "text/plain",
						Body:        []byte{4, 5, 6},
					})
				if err != nil {
					log.Errorf(`%v,%v`, publishMessageFailed, err)
					break
				}
				time.Sleep(2 * time.Second)
			}
			err = c.ReturnChannel(obj)
			if err != nil {
				log.Fatal(err)
			}
		}
		c.Close()
	}()
	<-forever

}

func TestConsume(t *testing.T) {
	forever := make(chan byte)
	go func() {
		c := New(url)
		if err := c.Open(); err != nil {
			log.Fatal(err)
		}
		for i := 0; i < 10; i++ {
			log.Infof("第%v次重试", i)
			obj, err := c.Channel()
			if err != nil {
				log.Error("获取Channel失败！", err)
				continue
			}
			ch := obj.(*amqp.Channel)
			q, err := ch.QueueDeclare(
				"test",
				false,
				true,
				false,
				false,
				nil,
			)
			if err != nil {
				log.Errorf(`%v,%v`, declareQueueFailed, err)
				return
			}
			msgs, err := ch.Consume(
				q.Name,
				"",
				true,
				false,
				false,
				false,
				nil)
			if err != nil {
				log.Errorf(`%v,%v`, publishMessageFailed, err)
				continue
			}

			for msg := range msgs {
				log.Info(msg.Body)
			}
		}
	}()
	<-forever
}
