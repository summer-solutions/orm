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
	channel := engine.rabbitMQChannels[r.QueueName]
	routerKey := channel.config.Name
	var headers amqp.Table
	if channel.config.Router != "" {
		routerKey = queueCode
	} else {
		headers = amqp.Table{"q-code": queueCode}
	}
	for _, value := range values {
		msg := amqp.Publishing{
			ContentType: "text/plain",
			Body:        value,
			Headers:     headers,
		}
		err := channel.publish(false, false, routerKey, msg)
		if err != nil {
			return err
		}
	}
	return nil
}
