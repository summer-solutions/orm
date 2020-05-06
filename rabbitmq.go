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

type rabbitMQConnection struct {
	config *rabbitMQConfig
	client *amqp.Connection
}

type rabbitMQChannelToQueue struct {
	connection      *rabbitMQConnection
	config          *RabbitMQQueueConfig
	channelSender   *amqp.Channel
	channelReceiver *amqp.Channel
	q               amqp.Queue
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

func (c *rabbitMQChannelToQueue) Close() bool {
	has := false
	if c.channelSender != nil {
		_ = c.channelSender.Close()
		has = true
	}
	if c.channelReceiver != nil {
		_ = c.channelReceiver.Close()
		has = true
	}
	return has
}

func (c *rabbitMQChannelToQueue) publish(mandatory, immediate bool, msg amqp.Publishing) error {
	err := c.initChannelSender()
	if err != nil {
		return err
	}
	return c.channelSender.Publish("", c.q.Name, mandatory, immediate, msg)
}

func (c *rabbitMQChannelToQueue) consume(consumer string, autoAck, exclusive, noLocal,
	noWait bool, args map[string]interface{}) (<-chan amqp.Delivery, error) {
	err := c.initChannelReceiver()
	if err != nil {
		return nil, err
	}
	return c.channelReceiver.Consume(c.q.Name, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (c *rabbitMQChannelToQueue) registerQueue(channel *amqp.Channel) error {
	config := c.config
	q, err := channel.QueueDeclare(config.Name, config.Durable, config.AutoDelete, config.Exclusive, config.NoWait, config.Arguments)
	if err != nil {
		return err
	}
	c.q = q
	return channel.Qos(config.PrefetchCount, config.PrefetchSize, false)
}

func (c *rabbitMQChannelToQueue) initChannelSender() error {
	if c.channelSender == nil {
		channel, err := c.initChannel()
		if err != nil {
			return errors.Trace(err)
		}
		c.channelSender = channel
	}
	return nil
}

func (c *rabbitMQChannelToQueue) initChannelReceiver() error {
	if c.channelReceiver == nil {
		channel, err := c.initChannel()
		if err != nil {
			return errors.Trace(err)
		}
		c.channelReceiver = channel
	}
	return nil
}

func (c *rabbitMQChannelToQueue) initChannel() (*amqp.Channel, error) {
	channel, err := c.connection.client.Channel()
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = c.registerQueue(channel)
	if err != nil {
		return nil, err
	}
	return channel, nil
}

type RabbitMQChannel struct {
	engine     *Engine
	channel    *rabbitMQChannelToQueue
	log        *log.Entry
	logHandler *multi.Handler
}

func (r *RabbitMQChannel) Publish(mandatory, immediate bool, msg amqp.Publishing) error {
	start := time.Now()
	err := r.channel.publish(mandatory, immediate, msg)
	if err != nil {
		return err
	}
	if r.log != nil {
		r.fillLogFields(start, "publish").WithField("mandatory", mandatory).
			WithField("immediate", immediate).Info("[ORM][RABBIT_MQ][PUBLISH]")
	}
	return nil
}

func (r *RabbitMQChannel) Consume(consumer string, autoAck, exclusive, noLocal, noWait bool, args map[string]interface{}) (<-chan amqp.Delivery, error) {
	start := time.Now()
	delivery, err := r.channel.consume(consumer, autoAck, exclusive, noLocal, noWait, args)
	if err != nil {
		return nil, err
	}
	if r.log != nil {
		r.fillLogFields(start, "consume").WithField("autoAck", autoAck).
			WithField("exclusive", exclusive).WithField("noLocal", noLocal).
			WithField("noWait", noWait).WithField("consumer", consumer).WithField("args", args).
			Info("[ORM][RABBIT_MQ][CONSUME]")
	}
	return delivery, nil
}

func (r *RabbitMQChannel) Close() {
	start := time.Now()
	closed := r.channel.Close()
	if closed && r.log != nil {
		r.fillLogFields(start, "close").Info("[ORM][RABBIT_MQ][CLOSE]")
	}
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
		WithField("Channel", r.channel.config.Name).
		WithField("microseconds", time.Since(start).Microseconds()).
		WithField("operation", operation).
		WithField("pool", r.channel.connection.config.code).
		WithField("target", "rabbitMQ").
		WithField("time", start.Unix())
	return e
}
