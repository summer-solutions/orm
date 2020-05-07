package orm

import "github.com/streadway/amqp"

type QueueSender interface {
	Send(engine *Engine, queueCode string, values [][]byte) error
}

type RedisQueueSender struct {
	PoolName string
}

func (r *RedisQueueSender) Send(engine *Engine, queueCode string, values [][]byte) error {
	members := make([]interface{}, len(values))
	for i, val := range values {
		members[i] = val
	}
	_, err := engine.GetRedis(r.PoolName).LPush(queueCode, members...)
	return err
}

type RabbitMQQueueSender struct {
	QueueName string
}

func (r *RabbitMQQueueSender) Send(engine *Engine, queueCode string, values [][]byte) error {
	channel := engine.GetRabbitMQChannel(r.QueueName)
	for _, value := range values {
		msg := amqp.Publishing{
			ContentType: "text/plain",
			Body:        value,
		}
		err := channel.PublishToExchange(false, false, queueCode, msg)
		if err != nil {
			return err
		}
	}
	return nil
}
