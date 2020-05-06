package orm

import (
	"os"
	"time"

	"github.com/juju/errors"

	"github.com/apex/log"
	"github.com/apex/log/handlers/multi"
	"github.com/apex/log/handlers/text"
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

type rabbitMQChannelToExchange struct {
	connection *rabbitMQConnection
	config     *RabbitMQExchangeConfig
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
			r.fillLogFields(start, "close channel").WithField("Queue", r.q.Name).Info("[ORM][RABBIT_MQ][CLOSE CHANNEL]")
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
	channel, q, err := r.initChannel(r.config.Name)
	if err != nil {
		return nil, err
	}
	receiver := &rabbitMQReceiver{name: name, channel: channel, q: q, parent: r}
	r.channelReceivers[q.Name] = receiver
	return receiver, nil
}

func (r *RabbitMQChannel) initChannel(name string) (*amqp.Channel, *amqp.Queue, error) {
	start := time.Now()
	channel, err := r.connection.client.Channel()
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if r.log != nil {
		r.fillLogFields(start, "create channel").Info("[ORM][RABBIT_MQ][CREATE CHANNEL]")
	}
	start = time.Now()
	q, err := r.registerQueue(channel, name)
	if err != nil {
		return nil, nil, err
	}
	if r.log != nil {
		r.fillLogFields(start, "register queue").WithField("Queue", q.Name).Info("[ORM][RABBIT_MQ][REGISTER QUEUE]")
	}
	return channel, q, nil
}

func (r *RabbitMQChannel) initChannelSender() error {
	if r.channelSender == nil {
		channel, q, err := r.initChannel(r.config.Name)
		if err != nil {
			return errors.Trace(err)
		}
		r.q = q
		r.channelSender = channel
	}
	return nil
}

func (r *RabbitMQChannel) Publish(mandatory, immediate bool, msg amqp.Publishing) error {
	err := r.initChannelSender()
	if err != nil {
		return err
	}
	start := time.Now()
	err = r.channelSender.Publish("", r.q.Name, mandatory, immediate, msg)
	if err != nil {
		return err
	}
	if r.log != nil {
		r.fillLogFields(start, "publish").WithField("Queue", r.q.Name).WithField("mandatory", mandatory).
			WithField("immediate", immediate).Info("[ORM][RABBIT_MQ][PUBLISH]")
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
