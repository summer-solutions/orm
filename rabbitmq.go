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

type RabbitMQReceiver interface {
	Close()
	Consume(autoAck, noLocal bool) (<-chan amqp.Delivery, error)
}

type rabbitMQReceiver struct {
	name    string
	q       *amqp.Queue
	channel *amqp.Channel
	parent  *RabbitMQChannel
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

func (r *rabbitMQReceiver) Consume(autoAck, noLocal bool) (<-chan amqp.Delivery, error) {
	start := time.Now()
	delivery, err := r.consume(autoAck, noLocal)
	if err != nil {
		return nil, err
	}
	if r.parent.log != nil {
		r.parent.fillLogFields(start, "consume").WithField("Queue", r.q.Name).WithField("autoAck", autoAck).
			WithField("exclusive", r.parent.config.Exclusive).WithField("noLocal", noLocal).
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
	PrefetchSize  int
	Exchange      string
	ExchangeKeys  []string
	Arguments     map[string]interface{}
}

type RabbitMQExchangeConfig struct {
	Name       string
	Type       string
	Durable    bool
	AutoDelete bool
	Internal   bool
	NoWait     bool
	Arguments  map[string]interface{}
}

func (r *RabbitMQChannel) Close() bool {
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
	if r.channelReceivers != nil {
		for _, receiver := range r.channelReceivers {
			receiver.Close()
		}
		r.channelReceivers = nil
		has = true
	}
	return has
}

func (r *RabbitMQChannel) registerQueue(channel *amqp.Channel, name string) (*amqp.Queue, error) {
	config := r.config
	q, err := channel.QueueDeclare(name, config.Durable, config.AutoDelete, config.Exclusive, config.NoWait, config.Arguments)
	if err != nil {
		return nil, err
	}
	err = channel.Qos(config.PrefetchCount, config.PrefetchSize, false)
	if err != nil {
		return nil, err
	}
	return &q, nil
}

type RabbitMQChannel struct {
	engine           *Engine
	log              *log.Entry
	logHandler       *multi.Handler
	channelSender    *amqp.Channel
	connection       *rabbitMQConnection
	channelReceivers map[string]RabbitMQReceiver
	config           *RabbitMQQueueConfig
	q                *amqp.Queue
}

func (r *RabbitMQChannel) NewConsumer(name string) (RabbitMQReceiver, error) {
	if r.channelReceivers == nil {
		r.channelReceivers = make(map[string]RabbitMQReceiver)
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
	r.channelReceivers[q.Name] = receiver
	return receiver, nil
}

func (r *RabbitMQChannel) initChannel(queueName string, sender bool) (*amqp.Channel, *amqp.Queue, error) {
	start := time.Now()
	channel, err := r.connection.client.Channel()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if r.log != nil {
		r.fillLogFields(start, "create channel").Info("[ORM][RABBIT_MQ][CREATE CHANNEL]")
	}
	if r.config.Exchange != "" {
		configExchange := r.engine.registry.rabbitMQExchangeConfigs[r.config.Exchange]
		start = time.Now()
		err := channel.ExchangeDeclare(configExchange.Name, configExchange.Type, configExchange.Durable, configExchange.AutoDelete,
			configExchange.Internal, configExchange.NoWait, nil)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if r.log != nil {
			r.fillLogFields(start, "register exchange").WithField("Name", configExchange.Name).
				WithField("Name", configExchange.Name).WithField("type", configExchange.Type).
				WithField("durable", configExchange.Durable).WithField("autodelete", configExchange.AutoDelete).
				WithField("internal", configExchange.Internal).WithField("nowait", configExchange.NoWait).
				Info("[ORM][RABBIT_MQ][REGISTER EXCHANGE]")
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
	if r.config.Exchange != "" {
		keys := r.config.ExchangeKeys
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

func (r *RabbitMQChannel) initChannelSender() error {
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

func (r *RabbitMQChannel) Publish(mandatory, immediate bool, routingKey string, msg amqp.Publishing) error {
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
				WithField("immediate", immediate).Info("[ORM][RABBIT_MQ][PUBLISH]")
		} else {
			r.fillLogFields(start, "publish").WithField("Queue", r.q.Name).WithField("mandatory", mandatory).
				WithField("immediate", immediate).Info("[ORM][RABBIT_MQ][PUBLISH]")
		}
	}
	return nil
}

func (r *RabbitMQChannel) AddLogger(handler log.Handler) {
	r.logHandler.Handlers = append(r.logHandler.Handlers, handler)
}

func (r *RabbitMQChannel) SetLogLevel(level log.Level) {
	logger := log.Logger{Handler: r.logHandler, Level: level}
	r.log = logger.WithField("source", "orm")
	r.log.Level = level
}

func (r *RabbitMQChannel) EnableDebug() {
	r.AddLogger(text.New(os.Stdout))
	r.SetLogLevel(log.DebugLevel)
}

func (r *RabbitMQChannel) Get(key string) (value string, has bool, err error) {
	return "", true, nil
}

func (r *RabbitMQChannel) fillLogFields(start time.Time, operation string) *log.Entry {
	e := r.log.
		WithField("microseconds", time.Since(start).Microseconds()).
		WithField("operation", operation).
		WithField("pool", r.connection.config.code).
		WithField("target", "rabbitMQ").
		WithField("time", start.Unix())
	return e
}
