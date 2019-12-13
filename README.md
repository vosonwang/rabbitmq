## RabbitMQ

这个库是对github.com/streadway/amqp的轻量化封装，提供了以下几个功能：
1. 断线重连（默认3次）
2. channel池

## 最佳实践
根据官方建议：
1. 不要频繁的打开和关闭连接或者Channel，尽可能少的建立连接和Channel，尽可能复用它们，每个进程中使用一个连接，在每个线程中使用一个Channel
2. Publish和Consume分别使用独立的连接
3. 不要在线程之间共享Channel，因为大部分客户端的Channels都不是线程安全的（因为会影响性能）。

## 说明
使用了[Go Commons Pool](https://github.com/jolestar/go-commons-pool)创建channel池

## 文献
感谢🙏：
[Hurricanezwf](https://github.com/Hurricanezwf/rabbitmq-go)
[OhBonsai](https://www.jianshu.com/p/da8c18bc3455)
