package orm

import (
	"sync"
	"time"

	log2 "github.com/apex/log"

	"github.com/juju/errors"

	"github.com/streadway/amqp"
)

const counterRabbitMQAll = "rabbitMQ.all"
const counterRabbitMQCloseChannel = "rabbitMQ.closeChannel"
const counterRabbitMQPublish = "rabbitMQ.publish"
const counterRabbitMQReceive = "rabbitMQ.receive"
const counterRabbitMQACK = "rabbitMQ.ack"
const counterRabbitMQConnect = "rabbitMQ.connect"
const counterRabbitMQCreateChannel = "rabbitMQ.createChannel"
const counterRabbitMQRegister = "rabbitMQ.register"

type rabbitMQConfig struct {
	code    string
	address string
}

type RabbitMQConsumer interface {
	Close()
	Consume(handler func(items [][]byte))
	DisableLoop()
	SetHeartBeat(beat func())
	SetMaxLoopDuration(duration time.Duration)
	Purge()
}

type rabbitMQReceiver struct {
	name            string
	channel         *amqp.Channel
	parent          *rabbitMQChannel
	disableLoop     bool
	maxLoopDuration time.Duration
	heartBeat       func()
}

func (r *rabbitMQReceiver) DisableLoop() {
	r.disableLoop = true
}

func (r *rabbitMQReceiver) Purge() {
	_, err := r.channel.QueuePurge(r.parent.config.Name, false)
	checkError(err)
}

func (r *rabbitMQReceiver) SetMaxLoopDuration(duration time.Duration) {
	r.maxLoopDuration = duration
}

func (r *rabbitMQReceiver) SetHeartBeat(beat func()) {
	r.heartBeat = beat
}

func (r *rabbitMQReceiver) Close() {
	start := time.Now()
	err := r.channel.Close()
	if r.parent.engine.queryLoggers[QueryLoggerSourceRabbitMQ] != nil {
		fillRabbitMQLogFields(r.parent.engine, "[ORM][RABBIT_MQ][CLOSE CHANNEL]", start, "close channel",
			map[string]interface{}{"Queue": r.parent.config.Name}, err)
	}
	if r.parent.engine.dataDog != nil {
		r.parent.engine.dataDog.incrementCounter(counterRabbitMQAll, 1)
		r.parent.engine.dataDog.incrementCounter(counterRabbitMQCloseChannel, 1)
	}
	delete(r.parent.connection.channelConsumers, r.parent.config.Name)
}

func (r *rabbitMQReceiver) consume() (<-chan amqp.Delivery, error) {
	return r.channel.Consume(r.parent.config.Name, r.name, false, false, false, false, nil)
}

func (r *rabbitMQReceiver) Consume(handler func(items [][]byte)) {
	delivery, err := r.consume()
	checkError(err)

	max := r.parent.config.PrefetchCount
	if max <= 0 {
		max = 1
	}
	counter := 0
	var last *amqp.Delivery
	items := make([][]byte, 0)
	beatTime := time.Now()
	loopTime := time.Now().UnixNano()
	for {
		now := time.Now()
		nowNano := now.UnixNano()
		timeOut := (nowNano - loopTime) >= r.maxLoopDuration.Nanoseconds()
		if counter > 0 && (timeOut || counter == max) {
			handler(items)
			items = nil
			start := time.Now()
			err = last.Ack(true)
			if r.parent.engine.queryLoggers[QueryLoggerSourceRabbitMQ] != nil {
				fillRabbitMQLogFields(r.parent.engine, "[ORM][RABBIT_MQ][ACK]", start, "ack",
					map[string]interface{}{"Queue": r.parent.config.Name, "consumer": r.name}, err)
			}
			loopTime = time.Now().UnixNano()
			if r.parent.engine.dataDog != nil {
				r.parent.engine.dataDog.incrementCounter(counterRabbitMQAll, 1)
				r.parent.engine.dataDog.incrementCounter(counterRabbitMQACK, 1)
				r.parent.engine.dataDog.incrementCounter(counterRabbitMQReceive, uint(len(items)))
			}
			checkError(err)
			counter = 0
			if r.disableLoop {
				if r.heartBeat != nil {
					r.heartBeat()
				}
				return
			}
		} else if timeOut && r.disableLoop {
			return
		}
		if r.heartBeat != nil && now.Sub(beatTime).Minutes() >= 1 {
			r.heartBeat()
			beatTime = now
		}
		select {
		case item := <-delivery:
			last = &item
			items = append(items, item.Body)
			counter++
		case <-time.After(time.Second):
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
		defer func() {
			if err := recover(); err != nil {
				engine.Log().Warn("rabbitMQ connection restarted", log2.Fields{"server": r.config.code})
			}
		}()
		<-errChannel
		r.connect(sender, engine)
	}()
}

func (r *rabbitMQConnection) connect(sender bool, engine *Engine) {
	start := time.Now()
	conn, err := amqp.Dial(r.config.address)
	if engine.queryLoggers[QueryLoggerSourceRabbitMQ] != nil {
		fillRabbitMQLogFields(engine, "[ORM][RABBIT_MQ][CONNECT]", start, "connect", nil, err)
	}
	if engine.dataDog != nil {
		engine.dataDog.incrementCounter(counterRabbitMQAll, 1)
		engine.dataDog.incrementCounter(counterRabbitMQConnect, 1)
	}
	checkError(err)
	if sender {
		r.clientSender = conn
	} else {
		r.clientReceivers = conn
	}
	errChannel := make(chan *amqp.Error)
	conn.NotifyClose(errChannel)

	go r.keepConnection(sender, engine, errChannel)
}

type RabbitMQQueueConfig struct {
	Name          string
	PrefetchCount int
	Router        string
	Durable       bool
	RouterKeys    []string
	AutoDelete    bool
	TTL           int
}

type RabbitMQRouterConfig struct {
	Name    string
	Type    string
	Durable bool
}

func (r *rabbitMQChannel) registerQueue(channel *amqp.Channel, name string) (*amqp.Queue, error) {
	config := r.config
	var args amqp.Table
	if config.TTL > 0 {
		args = amqp.Table{"x-message-ttl": int32(config.TTL * 1000)}
	}
	q, err := channel.QueueDeclare(name, config.Durable, config.AutoDelete, false, false, args)
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

func (r *RabbitMQQueue) Publish(body []byte) {
	msg := amqp.Publishing{
		ContentType:  "text/plain",
		Body:         body,
		DeliveryMode: amqp.Persistent,
	}
	r.publish(false, false, r.config.Name, msg)
}

type RabbitMQRouter struct {
	*rabbitMQChannel
}

func (r *RabbitMQRouter) Publish(routerKey string, body []byte) {
	msg := amqp.Publishing{
		ContentType:  "text/plain",
		Body:         body,
		DeliveryMode: amqp.Persistent,
	}
	r.publish(false, false, routerKey, msg)
}

type rabbitMQChannel struct {
	engine     *Engine
	connection *rabbitMQConnection
	config     *RabbitMQQueueConfig
}

func (r *rabbitMQChannel) NewConsumer(name string) RabbitMQConsumer {
	r.connection.muxConsumer.Lock()
	defer r.connection.muxConsumer.Unlock()
	if r.connection.channelConsumers == nil {
		r.connection.channelConsumers = make(map[string]RabbitMQConsumer)
	}
	queueName := r.config.Name
	channel := r.initChannel(queueName, false)
	receiver := &rabbitMQReceiver{name: name, channel: channel, parent: r, maxLoopDuration: time.Second}
	r.connection.channelConsumers[r.config.Name] = receiver
	return receiver
}

func (r *rabbitMQChannel) getClient(sender bool, force bool) *amqp.Connection {
	client := r.connection.getClient(sender)
	if client == nil || force {
		r.connection.muxConsumer.Lock()
		defer r.connection.muxConsumer.Unlock()
		client = r.connection.getClient(sender)
		if client == nil || client.IsClosed() {
			r.connection.connect(sender, r.engine)
		}
		return r.connection.getClient(sender)
	}
	return client
}

func (r *rabbitMQChannel) initChannel(queueName string, sender bool) *amqp.Channel {
	start := time.Now()
	client := r.getClient(sender, false)
	channel, err := client.Channel()
	if err != nil {
		rabbitErr, ok := err.(*amqp.Error)
		if ok && rabbitErr.Code == amqp.ChannelError {
			client = r.getClient(sender, true)
			channel, err = client.Channel()
		}
		if err != nil {
			if r.engine.queryLoggers[QueryLoggerSourceRabbitMQ] != nil {
				fillRabbitMQLogFields(r.engine, "[ORM][RABBIT_MQ][CREATE CHANNEL]", start, "create channel", map[string]interface{}{"Queue": queueName}, err)
			}
			if r.engine.dataDog != nil {
				r.engine.dataDog.incrementCounter(counterRabbitMQAll, 1)
				r.engine.dataDog.incrementCounter(counterRabbitMQCreateChannel, 1)
			}
			panic(err)
		}
	}
	if r.engine.queryLoggers[QueryLoggerSourceRabbitMQ] != nil {
		fillRabbitMQLogFields(r.engine, "[ORM][RABBIT_MQ][CREATE CHANNEL]", start, "create channel", map[string]interface{}{"Queue": queueName}, nil)
	}
	if r.engine.dataDog != nil {
		r.engine.dataDog.incrementCounter(counterRabbitMQAll, 1)
		r.engine.dataDog.incrementCounter(counterRabbitMQCreateChannel, 1)
	}
	hasRouter := r.config.Router != ""
	if hasRouter {
		configRouter := r.engine.registry.rabbitMQRouterConfigs[r.config.Router]
		typeValue := configRouter.Type
		var args amqp.Table
		start = time.Now()
		err := channel.ExchangeDeclare(configRouter.Name, typeValue, configRouter.Durable, r.config.AutoDelete,
			false, false, args)
		if r.engine.queryLoggers[QueryLoggerSourceRabbitMQ] != nil {
			fields := map[string]interface{}{"Name": configRouter.Name, "type": configRouter.Type, "args": args}
			fillRabbitMQLogFields(r.engine, "[ORM][RABBIT_MQ][REGISTER ROUTER]", start, "register", fields, err)
		}
		if r.engine.dataDog != nil {
			r.engine.dataDog.incrementCounter(counterRabbitMQAll, 1)
			r.engine.dataDog.incrementCounter(counterRabbitMQRegister, 1)
		}
		checkError(err)
	}
	start = time.Now()
	q, err := r.registerQueue(channel, queueName)
	if r.engine.queryLoggers[QueryLoggerSourceRabbitMQ] != nil {
		fillRabbitMQLogFields(r.engine, "[ORM][RABBIT_MQ][REGISTER QUEUE]", start, "register",
			map[string]interface{}{"Queue": queueName}, err)
	}
	if r.engine.dataDog != nil {
		r.engine.dataDog.incrementCounter(counterRabbitMQAll, 1)
		r.engine.dataDog.incrementCounter(counterRabbitMQRegister, 1)
	}
	checkError(err)
	if hasRouter {
		keys := r.config.RouterKeys
		if len(keys) == 0 {
			keys = append(keys, "")
		}
		for _, key := range keys {
			start = time.Now()
			err = channel.QueueBind(q.Name, key, r.config.Router, false, nil)
			if r.engine.queryLoggers[QueryLoggerSourceRabbitMQ] != nil {
				fillRabbitMQLogFields(r.engine, "[ORM][RABBIT_MQ][QUEUE BIND]", start, "register",
					map[string]interface{}{"Queue": q.Name, "Router": r.config.Router, "key": key}, err)
			}
			if r.engine.dataDog != nil {
				r.engine.dataDog.incrementCounter(counterRabbitMQAll, 1)
				r.engine.dataDog.incrementCounter(counterRabbitMQRegister, 1)
			}
			checkError(err)
		}
	}
	errChannel := make(chan amqp.Return)
	channel.NotifyReturn(errChannel)
	return channel
}

func (r *rabbitMQChannel) initChannelSender() {
	r.connection.muxSender.Do(func() {
		channel := r.initChannel(r.config.Name, true)
		r.connection.channelSender = channel
	})
}

func (r *rabbitMQChannel) publish(mandatory, immediate bool, routingKey string, msg amqp.Publishing) {
	if r.connection.channelSender == nil {
		r.initChannelSender()
	}
	start := time.Now()
	err := r.connection.channelSender.Publish(r.config.Router, routingKey, mandatory, immediate, msg)
	if err != nil {
		rabbitErr, ok := err.(*amqp.Error)
		if ok && rabbitErr.Code == amqp.ChannelError {
			r.connection.muxSender = sync.Once{}
			r.initChannelSender()
			err = r.connection.channelSender.Publish(r.config.Router, routingKey, mandatory, immediate, msg)
			checkError(err)
		}
	}
	if r.engine.queryLoggers[QueryLoggerSourceRabbitMQ] != nil {
		if r.config.Router != "" {
			fillRabbitMQLogFields(r.engine, "[ORM][RABBIT_MQ][PUBLISH]", start, "publish",
				map[string]interface{}{"Router": r.config.Router, "key": routingKey}, err)
		} else {
			fillRabbitMQLogFields(r.engine, "[ORM][RABBIT_MQ][PUBLISH]", start, "publish",
				map[string]interface{}{"Queue": r.config.Name, "key": routingKey}, err)
		}
	}
	if r.engine.dataDog != nil {
		r.engine.dataDog.incrementCounter(counterRabbitMQAll, 1)
		r.engine.dataDog.incrementCounter(counterRabbitMQPublish, 1)
	}
	checkError(err)
}

func fillRabbitMQLogFields(engine *Engine, message string, start time.Time, operation string, fields map[string]interface{}, err error) {
	now := time.Now()
	stop := time.Since(start).Microseconds()
	e := engine.queryLoggers[QueryLoggerSourceRabbitMQ].log.
		WithField("microseconds", stop).
		WithField("operation", operation).
		WithField("target", "rabbitMQ").
		WithField("started", start.UnixNano()).
		WithField("finished", now.UnixNano())
	for k, v := range fields {
		e = e.WithField(k, v)
	}
	if err != nil {
		injectLogError(err, e).Error(message)
	} else {
		e.Info(message)
	}
}
