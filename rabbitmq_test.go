package rabbitmq

import (
	"github.com/labstack/gommon/log"
	"github.com/streadway/amqp"
	"sync"
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
	mq := New(url)
	if err := mq.Open(); err != nil {
		log.Fatal(err)
	}
	var s sync.WaitGroup
	s.Add(1)
	go func() {
		ch, err := mq.Channel()
		if err != nil {
			log.Error("获取Channel失败！", err)
			err = mq.InvalidateChannel(ch)
			if err != nil {
				log.Fatal(err)
			}
			log.Fatal(err)
		}
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
			err = mq.InvalidateChannel(ch)
			if err != nil {
				log.Fatal(err)
			}
			return
		}
		for i := 0; i < 10; i++ {
			err = ch.Publish(
				"",
				q.Name,
				false,
				false,
				amqp.Publishing{
					ContentType: "text/plain",
					Body:        []byte{1, 2, 3},
				})
			if err != nil {
				log.Errorf(`%v,%v`, publishMessageFailed, err)
				err = mq.InvalidateChannel(ch)
				if err != nil {
					log.Fatal(err)
				}
			}
			time.Sleep(2 * time.Second)
		}
		err = mq.ReturnChannel(ch)
		if err != nil {
			log.Fatal(err)
		}
		s.Done()
	}()
	s.Add(1)
	go func() {
		for i := 0; i < 3; i++ {
			log.Infof("第%v次重试", i)
			ch, err := mq.Channel()
			if err != nil {
				log.Error("获取Channel失败！", err)
				continue
			}
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
					continue
				}
				time.Sleep(2 * time.Second)
			}
			err = mq.ReturnChannel(ch)
			if err != nil {
				log.Fatal(err)
			}
		}
		mq.Close()
		s.Done()
	}()
	s.Wait()

}

func TestConsume(t *testing.T) {
	forever := make(chan byte)
	go func() {
		for {
			time.Sleep(10 * time.Hour)
		}
	}()
	go func() {
		mq := New(url)
		if err := mq.Open(); err != nil {
			log.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			log.Infof("第%v次重试", i)
			ch, err := mq.Channel()
			if err != nil {
				log.Error("获取Channel失败！", err)
				err = mq.InvalidateChannel(ch)
				if err != nil {
					log.Error(err)
					continue
				}
				log.Error(err)
				continue
			}
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
				err = mq.InvalidateChannel(ch)
				if err != nil {
					log.Error(err)
					continue
				}
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
				err = mq.InvalidateChannel(ch)
				if err != nil {
					log.Error(err)
					continue
				}
				continue
			}

			for msg := range msgs {
				log.Info(msg.Body)
			}
		}

	}()
	<-forever
}
