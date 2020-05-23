package orm

import (
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/juju/errors"

	"github.com/apex/log"
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
	err := r.channel.Close()
	if r.parent.engine.loggers[LoggerSourceRabbitMQ] != nil {
		r.parent.fillLogFields("[ORM][RABBIT_MQ][CLOSE CHANNEL]", start, "close channel",
			map[string]interface{}{"Queue": r.parent.config.Name}, err)
	}
	delete(r.parent.connection.channelConsumers, r.parent.config.Name)
}

func (r *rabbitMQReceiver) consume() (<-chan amqp.Delivery, error) {
	return r.channel.Consume(r.parent.config.Name, r.name, false, false, false, false, nil)
}

func (r *rabbitMQReceiver) Consume(handler func(items [][]byte) error) error {
	start := time.Now()
	delivery, err := r.consume()
	if r.parent.engine.loggers[LoggerSourceRabbitMQ] != nil {
		r.parent.fillLogFields("[ORM][RABBIT_MQ][CONSUME]", start, "consume",
			map[string]interface{}{"Queue": r.parent.config.Name, "consumer": r.name}, err)
	}
	if err != nil {
		return errors.Trace(err)
	}

	timeOut := false
	max := r.parent.config.PrefetchCount
	if max <= 0 {
		max = 1
	}
	counter := 0
	var last *amqp.Delivery
	items := make([][]byte, 0)
	for {
		if counter > 0 && (timeOut || counter == max) {
			err := handler(items)
			items = nil
			if err != nil {
				return errors.Trace(err)
			}
			err = last.Ack(true)
			if r.parent.engine.loggers[LoggerSourceRabbitMQ] != nil {
				r.parent.fillLogFields("[ORM][RABBIT_MQ][ACK]", start, "ack",
					map[string]interface{}{"Queue": r.parent.config.Name, "consumer": r.name}, err)
			}
			if err != nil {
				return errors.Trace(err)
			}
			counter = 0
			timeOut = false
			if r.disableLoop {
				return nil
			}
		} else if timeOut && r.disableLoop {
			return nil
		}
		select {
		case item := <-delivery:
			last = &item
			items = append(items, item.Body)
			counter++
			if r.parent.engine.loggers[LoggerSourceRabbitMQ] != nil {
				r.parent.fillLogFields("[ORM][RABBIT_MQ][RECEIVED]", start, "receive",
					map[string]interface{}{"Queue": r.parent.config.Name, "consumer": r.name}, nil)
			}
		case <-time.After(r.maxLoopDuration):
			timeOut = true
		}
	}
}

type rabbitMQConnection struct {
	config           *rabbitMQConfig
	clientSender     *amqp.Connection
	clientReceivers  *amqp.Connection
	channelSender    *amqp.Channel
	channelConsumers map[string]RabbitMQConsumer
	muxConsumer      sync.Mutex
	muxSender        sync.Once
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

func (r *rabbitMQConnection) keepConnection(sender bool, engine *Engine, errChannel chan *amqp.Error) {
	go func() {
		err := <-errChannel
		if engine.loggers[LoggerSourceRabbitMQ] != nil {
			log.
				WithField("operation", "reconnect").
				WithField("target", "rabbitMQ").
				WithField("reason", err.Reason).
				WithField("time", time.Now().Unix()).Warn("[ORM][RABBIT_MQ][RECONNECT]")
		}
		_ = r.connect(sender, engine)
	}()
}

func (r *rabbitMQConnection) connect(sender bool, engine *Engine) error {
	start := time.Now()
	conn, err := amqp.Dial(r.config.address)
	if err != nil {
		return errors.Trace(err)
	}
	if engine.loggers[LoggerSourceRabbitMQ] != nil {
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

	go r.keepConnection(sender, engine, errChannel)
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
	engine     *Engine
	connection *rabbitMQConnection
	config     *RabbitMQQueueConfig
}

func (r *rabbitMQChannel) NewConsumer(name string) (RabbitMQConsumer, error) {
	r.connection.muxConsumer.Lock()
	defer r.connection.muxConsumer.Unlock()
	if r.connection.channelConsumers == nil {
		r.connection.channelConsumers = make(map[string]RabbitMQConsumer)
	}
	queueName := r.config.Name
	channel, err := r.initChannel(queueName, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	receiver := &rabbitMQReceiver{name: name, channel: channel, parent: r, maxLoopDuration: time.Second}
	r.connection.channelConsumers[r.config.Name] = receiver
	return receiver, nil
}

func (r *rabbitMQChannel) getClient(sender bool, force bool) (*amqp.Connection, error) {
	client := r.connection.getClient(sender)
	if client == nil || force {
		r.connection.muxConsumer.Lock()
		defer r.connection.muxConsumer.Unlock()
		client = r.connection.getClient(sender)
		if client == nil || client.IsClosed() {
			start := time.Now()
			err := r.connection.connect(sender, r.engine)
			if err != nil {
				if r.engine.loggers[LoggerSourceRabbitMQ] != nil {
					r.fillLogFields("[ORM][RABBIT_MQ][CONNECT]", start, "connect", nil, nil)
				}
				return nil, errors.Trace(err)
			}
		}
		return r.connection.getClient(sender), nil
	}
	return client, nil
}

func (r *rabbitMQChannel) initChannel(queueName string, sender bool) (*amqp.Channel, error) {
	start := time.Now()
	client, err := r.getClient(sender, false)
	if err != nil {
		return nil, errors.Trace(err)
	}
	channel, err := client.Channel()
	if err != nil {
		rabbitErr, ok := err.(*amqp.Error)
		if ok && rabbitErr.Code == amqp.ChannelError {
			client, err = r.getClient(sender, true)
			if err != nil {
				return nil, errors.Trace(err)
			}
			channel, err = client.Channel()
		}
		if err != nil {
			if r.engine.loggers[LoggerSourceRabbitMQ] != nil {
				r.fillLogFields("[ORM][RABBIT_MQ][CREATE CHANNEL]", start, "create channel", map[string]interface{}{"Queue": queueName}, err)
			}
			return nil, errors.Trace(err)
		}
	}
	if r.engine.loggers[LoggerSourceRabbitMQ] != nil {
		r.fillLogFields("[ORM][RABBIT_MQ][CREATE CHANNEL]", start, "create channel", map[string]interface{}{"Queue": queueName}, nil)
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
		if r.engine.loggers[LoggerSourceRabbitMQ] != nil {
			r.fillLogFields("[ORM][RABBIT_MQ][REGISTER ROUTER]", start, "register router",
				map[string]interface{}{"Name": configRouter.Name, "type": configRouter.Type, "args": args}, err)
		}
		if err != nil {
			return nil, errors.Trace(err)
		}
		if sender {
			return channel, nil
		}
	}
	start = time.Now()
	q, err := r.registerQueue(channel, queueName)
	if r.engine.loggers[LoggerSourceRabbitMQ] != nil {
		r.fillLogFields("[ORM][RABBIT_MQ][REGISTER QUEUE]", start, "register queue",
			map[string]interface{}{"Queue": queueName}, err)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	if hasRouter {
		keys := r.config.RouterKeys
		if len(keys) == 0 {
			keys = append(keys, "")
		}
		for _, key := range keys {
			start = time.Now()
			err = channel.QueueBind(q.Name, key, r.config.Router, false, nil)
			if r.engine.loggers[LoggerSourceRabbitMQ] != nil {
				r.fillLogFields("[ORM][RABBIT_MQ][QUEUE BIND]", start, "queue bind",
					map[string]interface{}{"Queue": q.Name, "Router": r.config.Router, "key": key}, err)
			}
			if err != nil {
				return nil, errors.Trace(err)
			}
		}
	}
	return channel, nil
}

func (r *rabbitMQChannel) initChannelSender() error {
	var err error
	r.connection.muxSender.Do(func() {
		channel, e := r.initChannel(r.config.Name, true)
		if e != nil {
			err = e
			return
		}
		r.connection.channelSender = channel
	})
	return err
}

func (r *rabbitMQChannel) publish(mandatory, immediate bool, routingKey string, msg amqp.Publishing) error {
	if r.connection.channelSender == nil {
		err := r.initChannelSender()
		if err != nil {
			return errors.Trace(err)
		}
	}
	start := time.Now()
	err := r.connection.channelSender.Publish(r.config.Router, routingKey, mandatory, immediate, msg)
	if err != nil {
		rabbitErr, ok := err.(*amqp.Error)
		if ok && rabbitErr.Code == amqp.ChannelError {
			r.connection.muxSender = sync.Once{}
			err2 := r.initChannelSender()
			if err2 != nil {
				return errors.Trace(err2)
			}
			err = r.connection.channelSender.Publish(r.config.Router, routingKey, mandatory, immediate, msg)
			if err != nil {
				return errors.Trace(err)
			}
		}
	}
	if r.engine.loggers[LoggerSourceRabbitMQ] != nil {
		if r.config.Router != "" {
			r.fillLogFields("[ORM][RABBIT_MQ][PUBLISH]", start, "publish",
				map[string]interface{}{"Router": r.config.Router, "key": routingKey}, err)
		} else {
			r.fillLogFields("[ORM][RABBIT_MQ][PUBLISH]", start, "publish",
				map[string]interface{}{"Queue": r.config.Name, "key": routingKey}, err)
		}
	}
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (r *rabbitMQChannel) fillLogFields(message string, start time.Time, operation string, fields map[string]interface{}, err error) {
	now := time.Now()
	stop := time.Since(start).Microseconds()
	e := r.engine.loggers[LoggerSourceRabbitMQ].log.
		WithField("microseconds", stop).
		WithField("operation", operation).
		WithField("target", "rabbitMQ").
		WithField("started", start.UnixNano()).
		WithField("finished", now.UnixNano())
	for k, v := range fields {
		e = e.WithField(k, v)
	}
	if err != nil {
		stackParts := strings.Split(errors.ErrorStack(err), "\n")
		stack := strings.Join(stackParts[1:], "\\n")
		fullStack := strings.Join(strings.Split(string(debug.Stack()), "\n")[4:], "\\n")
		e.WithError(err).
			WithField("stack", stack).
			WithField("stack_full", fullStack).
			WithField("error_type", reflect.TypeOf(errors.Cause(err)).String()).
			Error(message)
	} else {
		e.Info(message)
	}
}
