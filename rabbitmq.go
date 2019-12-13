package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"github.com/jolestar/go-commons-pool/v2"
	"github.com/labstack/gommon/log"
	"github.com/streadway/amqp"
	"reflect"
	"sync"
	"time"
)

const (
	maxRetry       = 10
	reconnectDelay = 5 * time.Second // 连接断开后多久重连
	poolClosed     = "重试多次后，Channel池仍然处于关闭状态"
	borrowFailed   = "Failed to borrow from pool"
)

type MQ struct {
	url        string
	connection *amqp.Connection
	closeChan  chan byte
	pool       *pool.ObjectPool
	ctx        context.Context
	mutex      sync.RWMutex     // 保护内部数据并发读写
	connNotify chan *amqp.Error // connection监听器
}

func init() {
	log.SetPrefix("RabbitMQ")
}

func New(url string) *MQ {
	var mq MQ
	mq.url = url
	return &mq
}

func (mq *MQ) Open() error {
	mq.mutex.Lock()
	defer mq.mutex.Unlock()
	conn, err := amqp.Dial(mq.url)
	if err != nil {
		return err
	}
	mq.closeChan = make(chan byte)
	mq.connection = conn
	// 对connection的close事件加监听器
	mq.connNotify = mq.connection.NotifyClose(make(chan *amqp.Error))
	mq.ctx = context.Background()
	mq.pool = pool.NewObjectPoolWithDefaultConfig(mq.ctx, pool.NewPooledObjectFactorySimple(
		func(context.Context) (interface{}, error) {
			ch, err := mq.channel()
			if err != nil {
				return nil, err
			}
			return ch, nil
		}))
	go mq.keepAlive()
	return err
}

func (mq *MQ) keepAlive() {
	select {
	case <-mq.closeChan:
		// 关闭Channel池
		mq.pool.Close(mq.ctx)
		log.Info("连接正常关闭")
		return
	case err := <-mq.connNotify:
		// 关闭Channel池
		mq.pool.Close(mq.ctx)
		// 连接被关闭
		log.Error("Connection Disconnected,Error: ", err)
		for i := 0; i < maxRetry; i++ {
			if e := mq.Open(); e != nil {
				log.Errorf(`Connection recover failed for %d times,Error:%v`, i+1, e)
				time.Sleep(reconnectDelay)
				continue
			}
			log.Info("Connection recover OK.")
			return
		}
		log.Errorf("Try to reconnect to MQ failed over maxRetry(%d), so exit.", maxRetry)
	}
}

func (mq *MQ) Close() {
	close(mq.closeChan)
	mq.connection.Close()
}

func (mq *MQ) channel() (*amqp.Channel, error) {
	mq.mutex.RLock()
	defer mq.mutex.RUnlock()
	return mq.connection.Channel()
}

func (mq *MQ) Channel() (*amqp.Channel, error) {
	for i := 0; i < maxRetry; i++ {
		// 判断Channel池是否处于关闭状态
		if !mq.pool.IsClosed() {
			log.Info(mq.pool.GetNumActive())
			obj, err := mq.pool.BorrowObject(mq.ctx)
			if err != nil {
				log.Error(borrowFailed, err)
				continue
			}
			log.Info(mq.pool.GetNumActive())
			ch, ok := obj.(*amqp.Channel)
			if !ok {
				return nil, errors.New(fmt.Sprintf("Channel池中的对象类型错误！期望/实际：*amqp.Channel/%v", reflect.TypeOf(obj)))
			}
			return ch, nil
		}
		time.Sleep(reconnectDelay)
	}
	return nil, errors.New(poolClosed)
}

func (mq *MQ) ReturnChannel(a *amqp.Channel) error {
	log.Info(mq.pool.GetNumActive())
	err := mq.pool.ReturnObject(mq.ctx, a)
	if err != nil {
		log.Error("归还Channel失败！")
		return err
	}
	return nil
}

func (mq *MQ) InvalidateChannel(a *amqp.Channel) error {
	log.Info(mq.pool.GetNumActive())
	err := mq.pool.InvalidateObject(mq.ctx, a)
	if err != nil {
		log.Error("剔除无效Channel失败！")
		return err
	}
	return nil
}
