package orm

import (
	"os"
	"sync"
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

type RabbitMQConsumer interface {
	Close()
	Consume(handler func(items [][]byte) error) error
	DisableLoop()
}

type rabbitMQReceiver struct {
	name            string
	q               *amqp.Queue
	channel         *amqp.Channel
	parent          *rabbitMQChannel
	disableLoop     bool
	maxLoopDuration time.Duration
}

func (r *rabbitMQReceiver) DisableLoop() {
	r.disableLoop = true
}

func (r *rabbitMQReceiver) SetMaxLoopDudation(duration time.Duration) {
	r.maxLoopDuration = duration
}

func (r *rabbitMQReceiver) Close() {
	start := time.Now()
	_ = r.channel.Close()
	if r.parent.log != nil {
		r.parent.fillLogFields(start, "close channel").WithField("Queue", r.q.Name).Info("[ORM][RABBIT_MQ][CLOSE CHANNEL]")
	}
	delete(r.parent.channelConsumers, r.q.Name)
}

func (r *rabbitMQReceiver) consume() (<-chan amqp.Delivery, error) {
	return r.channel.Consume(r.q.Name, r.name, false, false, false, false, nil)
}

func (r *rabbitMQReceiver) Consume(handler func(items [][]byte) error) error {
	start := time.Now()
	delivery, err := r.consume()
	if err != nil {
		return errors.Trace(err)
	}
	if r.parent.log != nil {
		r.parent.fillLogFields(start, "consume").WithField("Queue", r.q.Name).
			WithField("consumer", r.name).Info("[ORM][RABBIT_MQ][CONSUME]")
	}

	timeOut := false
	max := r.parent.config.PrefetchCount
	if max <= 0 {
		max = 1
	}
	counter := 0
	items := make([][]byte, 0, max)
	var last *amqp.Delivery
	for {
		if counter > 0 && (timeOut || counter == max) {
			err := handler(items)
			if err != nil {
				return errors.Trace(err)
			}
			err = last.Ack(true)
			if err != nil {
				return errors.Trace(err)
			}
			if r.parent.log != nil {
				r.parent.fillLogFields(start, "ack").WithField("Queue", r.q.Name).
					WithField("consumer", r.name).Info("[ORM][RABBIT_MQ][ACK]")
			}
			counter = 0
			timeOut = false
			items = items[:0]
			if r.disableLoop {
				return nil
			}
		} else if timeOut && r.disableLoop {
			return nil
		}
		select {
		case item := <-delivery:
			counter++
			last = &item
			items = append(items, item.Body)
			if r.parent.log != nil {
				r.parent.fillLogFields(start, "received").WithField("Queue", r.q.Name).
					WithField("consumer", r.name).Info("[ORM][RABBIT_MQ][RECEIVED]")
			}
		case <-time.After(r.maxLoopDuration):
			timeOut = true
		}
	}
}

type rabbitMQConnection struct {
	config          *rabbitMQConfig
	clientSender    *amqp.Connection
	clientReceivers *amqp.Connection
	mux             sync.Mutex
}

type rabbitMQChannelToQueue struct {
	connection *rabbitMQConnection
	config     *RabbitMQQueueConfig
}

func (r *rabbitMQConnection) getClient(sender bool) *amqp.Connection {
	if sender {
		return r.clientSender
	}
	return r.clientReceivers
}

func (r *rabbitMQConnection) keepConnection(sender bool, log *log.Entry, errChannel chan *amqp.Error) {
	go func() {
		err := <-errChannel
		if log != nil {
			log.
				WithField("operation", "reconnect").
				WithField("target", "rabbitMQ").
				WithField("reason", err.Reason).
				WithField("time", time.Now().Unix()).Warn("[ORM][RABBIT_MQ][RECONNECT]")
		}
		_ = r.connect(sender, log)
	}()
}

func (r *rabbitMQConnection) connect(sender bool, log *log.Entry) error {
	start := time.Now()
	conn, err := amqp.Dial(r.config.address)
	if err != nil {
		return errors.Trace(err)
	}
	if log != nil {
		log.
			WithField("microseconds", time.Since(start).Microseconds()).
			WithField("operation", "open connection").
			WithField("target", "rabbitMQ").
			WithField("time", start.Unix()).Info("[ORM][RABBIT_MQ][OPEN CONNECTION]")
	}
	if sender {
		r.clientSender = conn
	} else {
		r.clientReceivers = conn
	}
	errChannel := make(chan *amqp.Error)
	conn.NotifyClose(errChannel)

	go r.keepConnection(sender, log, errChannel)
	return nil
}

type RabbitMQQueueConfig struct {
	Name          string
	PrefetchCount int
	Delayed       bool
	Router        string
	Durable       bool
	RouterKeys    []string
	AutoDelete    bool
}

type RabbitMQRouterConfig struct {
	Name    string
	Type    string
	Durable bool
}

func (r *rabbitMQChannel) registerQueue(channel *amqp.Channel, name string) (*amqp.Queue, error) {
	config := r.config
	q, err := channel.QueueDeclare(name, config.Durable, config.AutoDelete, false, false, nil)
	if err != nil {
		return nil, errors.Trace(err)
	}
	err = channel.Qos(config.PrefetchCount, 0, false)
	if err != nil {
		return nil, errors.Trace(err)
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
	channel, q, err := r.initChannel(queueName, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	receiver := &rabbitMQReceiver{name: name, channel: channel, q: q, parent: r, maxLoopDuration: time.Second}
	r.channelConsumers[q.Name] = receiver
	return receiver, nil
}

func (r *rabbitMQChannel) getClient(sender bool, force bool) (*amqp.Connection, error) {
	client := r.connection.getClient(sender)
	if client == nil || force {
		r.connection.mux.Lock()
		client = r.connection.getClient(sender)
		if client == nil || client.IsClosed() {
			err := r.connection.connect(sender, r.log)
			r.connection.mux.Unlock()
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
		return r.connection.getClient(sender), nil
	}
	return client, nil
}

func (r *rabbitMQChannel) initChannel(queueName string, sender bool) (*amqp.Channel, *amqp.Queue, error) {
	start := time.Now()
	client, err := r.getClient(sender, false)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	channel, err := client.Channel()
	if err != nil {
		rabbitErr, ok := err.(*amqp.Error)
		if ok && rabbitErr.Code == amqp.ChannelError {
			client, err = r.getClient(sender, true)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			channel, err = client.Channel()
		}
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
	}
	if r.log != nil {
		r.fillLogFields(start, "create channel").Info("[ORM][RABBIT_MQ][CREATE CHANNEL]")
	}
	hasRouter := r.config.Router != ""
	if hasRouter {
		configRouter := r.engine.registry.rabbitMQRouterConfigs[r.config.Router]
		typeValue := configRouter.Type
		var args amqp.Table
		if r.config.Delayed {
			args = amqp.Table{"x-delayed-type": configRouter.Type}
			typeValue = "x-delayed-message"
		}
		start = time.Now()
		err := channel.ExchangeDeclare(configRouter.Name, typeValue, configRouter.Durable, true,
			false, false, args)
		if err != nil {
			return nil, nil, errors.Trace(err)
		}
		if r.log != nil {
			r.fillLogFields(start, "register exchange").WithField("Name", configRouter.Name).
				WithField("Name", configRouter.Name).WithField("type", configRouter.Type).
				WithField("args", args).Info("[ORM][RABBIT_MQ][REGISTER EXCHANGE]")
		}
		if sender {
			return channel, nil, nil
		}
	}
	start = time.Now()
	q, err := r.registerQueue(channel, queueName)
	if err != nil {
		return nil, nil, errors.Trace(err)
	}
	if r.log != nil {
		r.fillLogFields(start, "register queue").WithField("Queue", q.Name).Info("[ORM][RABBIT_MQ][REGISTER QUEUE]")
	}
	if hasRouter {
		keys := r.config.RouterKeys
		if len(keys) == 0 {
			keys = append(keys, "")
		}
		for _, key := range keys {
			start = time.Now()
			err = channel.QueueBind(q.Name, key, r.config.Router, false, nil)
			if err != nil {
				return nil, nil, errors.Trace(err)
			}
			if r.log != nil {
				r.fillLogFields(start, "queue bind").WithField("Queue", q.Name).
					WithField("Router", r.config.Router).WithField("key", key).
					Info("[ORM][RABBIT_MQ][QUEUE BIND]")
			}
		}
	}
	return channel, q, nil
}

func (r *rabbitMQChannel) initChannelSender(force bool) error {
	if r.channelSender == nil || force {
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
	err := r.initChannelSender(false)
	if err != nil {
		return errors.Trace(err)
	}
	start := time.Now()
	err = r.channelSender.Publish(r.config.Router, routingKey, mandatory, immediate, msg)
	if err != nil {
		rabbitErr, ok := err.(*amqp.Error)
		if ok && rabbitErr.Code == amqp.ChannelError {
			err2 := r.initChannelSender(true)
			if err2 != nil {
				return errors.Trace(err2)
			}
			err = r.channelSender.Publish(r.config.Router, routingKey, mandatory, immediate, msg)
			if err != nil {
				return errors.Trace(err)
			}
		}
		if err != nil {
			return errors.Trace(err)
		}
	}
	if r.log != nil {
		if r.config.Router != "" {
			r.fillLogFields(start, "publish").WithField("Router", r.config.Router).WithField("mandatory", mandatory).
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
		WithField("target", "rabbitMQ").
		WithField("time", start.Unix())
	return e
}
