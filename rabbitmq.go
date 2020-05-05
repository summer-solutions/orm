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

type rabbitMQChannel struct {
	connection *rabbitMQConnection
	config     *RabbitMQChannelConfig
	channel    *amqp.Channel
	q          amqp.Queue
}

type RabbitMQChannelConfig struct {
	Name       string
	Passive    bool
	Durable    bool
	Exclusive  bool
	AutoDelete bool
	NoWait     bool
	Arguments  map[string]interface{}
}

func (c *rabbitMQChannel) Close() bool {
	if c.channel == nil {
		return false
	}
	err := c.channel.Close()
	if err != nil {
		panic(err)
	}
	c.channel = nil
	return true
}

func (c *rabbitMQChannel) publish(exchange string, mandatory, immediate bool, msg amqp.Publishing) error {
	err := c.initChannel()
	if err != nil {
		return err
	}
	return c.channel.Publish(exchange, c.q.Name, mandatory, immediate, msg)
}

func (c *rabbitMQChannel) consume(consumer string, autoAck, exclusive, noLocal,
	noWait bool, args map[string]interface{}) (<-chan amqp.Delivery, error) {
	err := c.initChannel()
	if err != nil {
		return nil, err
	}
	return c.channel.Consume(c.q.Name, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (c *rabbitMQChannel) registerQueue() error {
	err := c.initChannel()
	if err != nil {
		return err
	}
	defer c.Close()
	config := c.config
	c.q, err = c.channel.QueueDeclare(config.Name, config.Durable, config.AutoDelete, config.Exclusive, config.NoWait, config.Arguments)
	return err
}

func (c *rabbitMQChannel) initChannel() error {
	if c.channel == nil {
		channel, err := c.connection.client.Channel()
		if err != nil {
			return errors.Trace(err)
		}
		c.channel = channel
	}
	return nil
}

type RabbitMQ struct {
	engine     *Engine
	channel    *rabbitMQChannel
	log        *log.Entry
	logHandler *multi.Handler
}

func (r *RabbitMQ) Publish(exchange string, mandatory, immediate bool, msg amqp.Publishing) error {
	start := time.Now()
	err := r.channel.publish(exchange, mandatory, immediate, msg)
	if err != nil {
		return err
	}
	if r.log != nil {
		r.fillLogFields(start, "publish").WithField("mandatory", mandatory).
			WithField("immediate", immediate).WithField("exchange", exchange).
			Info("[ORM][RABBIT_MQ][PUBLISH]")
	}
	return nil
}

func (r *RabbitMQ) Consume(consumer string, autoAck, exclusive, noLocal, noWait bool, args map[string]interface{}) (<-chan amqp.Delivery, error) {
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

func (r *RabbitMQ) Close() {
	start := time.Now()
	closed := r.channel.Close()
	if closed && r.log != nil {
		r.fillLogFields(start, "close").Info("[ORM][RABBIT_MQ][CLOSE]")
	}
}

func (r *RabbitMQ) AddLogger(handler log.Handler) {
	r.logHandler.Handlers = append(r.logHandler.Handlers, handler)
}

func (r *RabbitMQ) SetLogLevel(level log.Level) {
	logger := log.Logger{Handler: r.logHandler, Level: level}
	r.log = logger.WithField("source", "orm")
	r.log.Level = level
}

func (r *RabbitMQ) EnableDebug() {
	r.AddLogger(text.New(os.Stdout))
	r.SetLogLevel(log.DebugLevel)
}

func (r *RabbitMQ) Get(key string) (value string, has bool, err error) {
	return "", true, nil
}

func (r *RabbitMQ) fillLogFields(start time.Time, operation string) *log.Entry {
	e := r.log.
		WithField("Channel", r.channel.config.Name).
		WithField("microseconds", time.Since(start).Microseconds()).
		WithField("operation", operation).
		WithField("pool", r.channel.connection.config.code).
		WithField("target", "rabbitMQ").
		WithField("time", start.Unix())
	return e
}
