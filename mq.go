package rabbitmq

import (
	"context"
	"errors"
	"github.com/jolestar/go-commons-pool/v2"
	"github.com/labstack/gommon/log"
	"github.com/streadway/amqp"
	"time"
)

const (
	maxRetry       = 1000
	reconnectDelay = 5 * time.Second // 连接断开后多久重连
	borrowFailed   = "Failed to borrow from pool"
)

type Connection struct {
	url        string
	connection *amqp.Connection
	closeChan  chan byte
	stopChan   chan byte
	pool       *pool.ObjectPool
	ctx        context.Context
	connNotify chan *amqp.Error // connection监听器
}

func init() {
	log.SetPrefix("RabbitMQ")
}

func New(url string) *Connection {
	return &Connection{
		url:       url,
		stopChan:  make(chan byte),
		closeChan: make(chan byte),
	}
}

func (c *Connection) Open() error {
	conn, err := amqp.Dial(c.url)
	if err != nil {
		return err
	}
	c.connection = conn
	// 对connection的close事件加监听器
	c.connNotify = c.connection.NotifyClose(make(chan *amqp.Error))
	c.ctx = context.Background()
	c.pool = pool.NewObjectPoolWithDefaultConfig(c.ctx, pool.NewPooledObjectFactorySimple(
		func(context.Context) (interface{}, error) {
			// 此处出现异常，会导致当前pool关闭，然后重连
			// 同时只要申请前判断pool是否关闭，就不会重新申请新的channel，不会导致新的异常，从而重连，进而导致死循环
			ch, err := c.connection.Channel()
			if err != nil {
				log.Error("打开新的Channel失败！", err)
				return nil, err
			}
			return ch, nil
		}))
	go c.keepAlive()
	return err
}

func (c *Connection) keepAlive() {
	select {
	case <-c.closeChan:
		// 关闭Channel池
		c.pool.Close(c.ctx)
		log.Info("Channel池正常关闭")
		c.connection.Close()
		log.Info("连接正常关闭")
	case err := <-c.connNotify:
		// 连接被关闭
		// 关闭Channel池
		c.pool.Close(c.ctx)
		log.Error("Connection Disconnected,Error: ", err)
		for i := 0; i < maxRetry; i++ {
			if e := c.Open(); e != nil {
				log.Errorf(`Connection recover failed for %d times,Error:%v`, i+1, e)
				time.Sleep(reconnectDelay)
				continue
			}
			log.Info("Connection recover OK.")
			return
		}
		// 此时mq彻底失效
		log.Errorf("Try to reconnect to RabbitMQ failed over maxRetry(%d), so exit.", maxRetry)
		close(c.stopChan)
	}
}

func (c *Connection) Close() {
	close(c.closeChan)
}

func (c *Connection) Channel() (interface{}, error) {
	for {
		select {
		case <-c.stopChan:
			return nil, errors.New("重连失败！")
		default:
			// 判断Channel池是否处于关闭状态，如果是，继续重试，如果否，可能是只是channel被关闭
			if !c.pool.IsClosed() {
				// 重新申请新的channel
				obj, err := c.pool.BorrowObject(c.ctx)
				if err != nil {
					log.Error(borrowFailed, err)
					return nil, err
				}
				return obj, nil
			}
		}
		time.Sleep(reconnectDelay)
	}
}

func (c *Connection) ReturnChannel(obj interface{}) error {
	err := c.pool.ReturnObject(c.ctx, obj)
	if err != nil {
		log.Error("归还Channel失败！")
		return err
	}
	return nil
}
