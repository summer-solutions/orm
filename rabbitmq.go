package orm

import (
	"fmt"
	"os"
	"time"

	"github.com/juju/errors"

	"github.com/apex/log"
	"github.com/apex/log/handlers/multi"
	"github.com/apex/log/handlers/text"
	"github.com/lithammer/shortuuid"
	"github.com/streadway/amqp"
)

type rabbitMQConfig struct {
	code    string
	address string
}

type RabbitMQConsumer interface {
	Close()
	Consume() (<-chan amqp.Delivery, error)
}

type rabbitMQReceiver struct {
	name    string
	q       *amqp.Queue
	channel *amqp.Channel
	parent  *rabbitMQChannel
}

func (r *rabbitMQReceiver) Close() {
	start := time.Now()
	_ = r.channel.Close()
	if r.parent.log != nil {
		r.parent.fillLogFields(start, "close channel").WithField("Queue", r.q.Name).Info("[ORM][RABBIT_MQ][CLOSE CHANNEL]")
	}
}

func (r *rabbitMQReceiver) consume(autoAck, noLocal bool) (<-chan amqp.Delivery, error) {
	return r.channel.Consume(r.q.Name, r.name, autoAck, r.parent.config.Exclusive, noLocal, r.parent.config.NoWait, nil)
}

func (r *rabbitMQReceiver) Consume() (<-chan amqp.Delivery, error) {
	start := time.Now()
	delivery, err := r.consume(false, false)
	if err != nil {
		return nil, err
	}
	if r.parent.log != nil {
		r.parent.fillLogFields(start, "consume").WithField("Queue", r.q.Name).WithField("autoAck", false).
			WithField("exclusive", r.parent.config.Exclusive).WithField("noLocal", false).
			WithField("noWait", r.parent.config.NoWait).WithField("consumer", r.name).Info("[ORM][RABBIT_MQ][CONSUME]")
	}
	return delivery, nil
}

type rabbitMQConnection struct {
	config *rabbitMQConfig
	client *amqp.Connection
}

type rabbitMQChannelToQueue struct {
	connection *rabbitMQConnection
	config     *RabbitMQQueueConfig
}

type RabbitMQQueueConfig struct {
	Name          string
	Passive       bool
	Durable       bool
	Exclusive     bool
	AutoDelete    bool
	NoWait        bool
	PrefetchCount int
	Exchange      string
	RouterKeys    []string
	Arguments     map[string]interface{}
}

type RabbitMQExchangeConfig struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Delayed    bool
	Arguments  map[string]interface{}
}

func (r *rabbitMQChannel) close() bool {
	has := false
	if r.channelSender != nil {
		start := time.Now()
		_ = r.channelSender.Close()
		if r.log != nil {
			if r.config.Exchange == "" {
				r.fillLogFields(start, "close channel").WithField("Queue", r.q.Name).Info("[ORM][RABBIT_MQ][CLOSE CHANNEL]")
			} else {
				r.fillLogFields(start, "close channel").WithField("Exchange", r.config.Exchange).Info("[ORM][RABBIT_MQ][CLOSE CHANNEL]")
			}
		}
		has = true
	}
	if r.channelConsumers != nil {
		for _, receiver := range r.channelConsumers {
			receiver.Close()
		}
		r.channelConsumers = nil
		has = true
	}
	return has
}

func (r *rabbitMQChannel) registerQueue(channel *amqp.Channel, name string) (*amqp.Queue, error) {
	config := r.config
	q, err := channel.QueueDeclare(name, config.Durable, config.AutoDelete, config.Exclusive, config.NoWait, config.Arguments)
	if err != nil {
		return nil, err
	}
	err = channel.Qos(config.PrefetchCount, 0, false)
	if err != nil {
		return nil, err
	}
	return &q, nil
}

type RabbitMQQueue struct {
	*rabbitMQChannel
}

func (r *RabbitMQQueue) Publish(body []byte) error {
	msg := amqp.Publishing{
		ContentType: "text/plain",
		Body:        body,
	}
	return r.publish(false, false, r.config.Name, msg)
}

type RabbitMQDelayedQueue struct {
	*rabbitMQChannel
}

func (r *RabbitMQDelayedQueue) Publish(delayed time.Duration, body []byte) error {
	msg := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		Headers:      amqp.Table{"x-delay": delayed.Milliseconds()},
		ContentType:  "text/plain",
		Body:         body,
	}
	return r.publish(false, false, r.config.Name, msg)
}

type RabbitMQRouter struct {
	*rabbitMQChannel
}

func (r *RabbitMQRouter) Publish(routerKey string, body []byte) error {
	msg := amqp.Publishing{
		ContentType: "text/plain",
		Body:        body,
	}
	return r.publish(false, false, routerKey, msg)
}

type rabbitMQChannel struct {
	engine           *Engine
	log              *log.Entry
	logHandler       *multi.Handler
	channelSender    *amqp.Channel
	connection       *rabbitMQConnection
	channelConsumers map[string]RabbitMQConsumer
	config           *RabbitMQQueueConfig
	q                *amqp.Queue
}

func (r *rabbitMQChannel) NewConsumer(name string) (RabbitMQConsumer, error) {
	if r.channelConsumers == nil {
		r.channelConsumers = make(map[string]RabbitMQConsumer)
	}
	queueName := r.config.Name
	if r.config.Exchange != "" {
		queueName = fmt.Sprintf("%s:%s", queueName, shortuuid.New())
	}
	channel, q, err := r.initChannel(queueName, false)
	if err != nil {
		return nil, err
	}
	receiver := &rabbitMQReceiver{name: name, channel: channel, q: q, parent: r}
	r.channelConsumers[q.Name] = receiver
	return receiver, nil
}

func (r *rabbitMQChannel) initChannel(queueName string, sender bool) (*amqp.Channel, *amqp.Queue, error) {
	start := time.Now()
	channel, err := r.connection.client.Channel()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if r.log != nil {
		r.fillLogFields(start, "create channel").Info("[ORM][RABBIT_MQ][CREATE CHANNEL]")
	}
	hasExchange := r.config.Exchange != ""
	if hasExchange {
		configExchange := r.engine.registry.rabbitMQExchangeConfigs[r.config.Exchange]
		typeValue := configExchange.Type
		var args amqp.Table
		if configExchange.Delayed {
			args = amqp.Table{"x-delayed-type": configExchange.Type}
			typeValue = "x-delayed-message"
		}
		start = time.Now()
		err := channel.ExchangeDeclare(configExchange.Name, typeValue, configExchange.Durable, configExchange.AutoDelete,
			configExchange.Internal, configExchange.NoWait, args)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if r.log != nil {
			r.fillLogFields(start, "register exchange").WithField("Name", configExchange.Name).
				WithField("Name", configExchange.Name).WithField("type", configExchange.Type).
				WithField("durable", configExchange.Durable).WithField("autodelete", configExchange.AutoDelete).
				WithField("internal", configExchange.Internal).WithField("nowait", configExchange.NoWait).
				WithField("args", args).Info("[ORM][RABBIT_MQ][REGISTER EXCHANGE]")
		}
		if sender {
			return channel, nil, nil
		}
	}
	start = time.Now()
	q, err := r.registerQueue(channel, queueName)
	if err != nil {
		return nil, nil, err
	}
	if r.log != nil {
		r.fillLogFields(start, "register queue").WithField("Queue", q.Name).Info("[ORM][RABBIT_MQ][REGISTER QUEUE]")
	}
	if hasExchange {
		keys := r.config.RouterKeys
		if len(keys) == 0 {
			keys = append(keys, "")
		}
		for _, key := range keys {
			start = time.Now()
			err = channel.QueueBind(q.Name, key, r.config.Exchange, r.config.NoWait, nil)
			if err != nil {
				return nil, nil, err
			}
			if r.log != nil {
				r.fillLogFields(start, "queue bind").WithField("Queue", q.Name).
					WithField("Exchange", r.config.Exchange).WithField("key", key).
					Info("[ORM][RABBIT_MQ][QUEUE BIND]")
			}
		}
	}
	return channel, q, nil
}

func (r *rabbitMQChannel) initChannelSender() error {
	if r.channelSender == nil {
		channel, q, err := r.initChannel(r.config.Name, true)
		if err != nil {
			return errors.Trace(err)
		}
		r.q = q
		r.channelSender = channel
	}
	return nil
}

func (r *rabbitMQChannel) publish(mandatory, immediate bool, routingKey string, msg amqp.Publishing) error {
	err := r.initChannelSender()
	if err != nil {
		return err
	}
	start := time.Now()
	err = r.channelSender.Publish(r.config.Exchange, routingKey, mandatory, immediate, msg)
	if err != nil {
		return err
	}
	if r.log != nil {
		if r.config.Exchange != "" {
			r.fillLogFields(start, "publish").WithField("Exchange", r.config.Exchange).WithField("mandatory", mandatory).
				WithField("immediate", immediate).WithField("key", routingKey).Info("[ORM][RABBIT_MQ][PUBLISH]")
		} else {
			r.fillLogFields(start, "publish").WithField("Queue", r.q.Name).WithField("mandatory", mandatory).
				WithField("immediate", immediate).WithField("key", routingKey).Info("[ORM][RABBIT_MQ][PUBLISH]")
		}
	}
	return nil
}

func (r *rabbitMQChannel) AddLogger(handler log.Handler) {
	r.logHandler.Handlers = append(r.logHandler.Handlers, handler)
}

func (r *rabbitMQChannel) SetLogLevel(level log.Level) {
	logger := log.Logger{Handler: r.logHandler, Level: level}
	r.log = logger.WithField("source", "orm")
	r.log.Level = level
}

func (r *rabbitMQChannel) EnableDebug() {
	r.AddLogger(text.New(os.Stdout))
	r.SetLogLevel(log.DebugLevel)
}

func (r *rabbitMQChannel) fillLogFields(start time.Time, operation string) *log.Entry {
	e := r.log.
		WithField("microseconds", time.Since(start).Microseconds()).
		WithField("operation", operation).
		WithField("pool", r.connection.config.code).
		WithField("target", "rabbitMQ").
		WithField("time", start.Unix())
	return e
}
