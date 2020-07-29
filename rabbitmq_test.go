package orm

import (
	"testing"
	"time"

	log2 "github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

func TestRabbitMQ(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRabbitMQServer("amqp://rabbitmq_user:rabbitmq_password@localhost:5677/test")
	registry.RegisterRabbitMQQueue(&RabbitMQQueueConfig{Name: "test_queue"})
	registry.RegisterRabbitMQRouter(&RabbitMQRouterConfig{Name: "test_exchange", Type: "fanout"})
	registry.RegisterRabbitMQQueue(&RabbitMQQueueConfig{Name: "test_queue_router_with_ttl", PrefetchCount: 2, Router: "test_exchange", TTL: 60})
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()
	logger := memory.New()
	engine.AddQueryLogger(logger, log2.DebugLevel, QueryLoggerSourceRabbitMQ)
	engine.DataDog().EnableORMAPMLog(log2.DebugLevel, true, QueryLoggerSourceRabbitMQ)

	queue := engine.GetRabbitMQQueue("test_queue")
	consumer := queue.NewConsumer("default_consumer")
	consumer.Purge()
	defer consumer.Close()
	queue.Publish([]byte("hello"))

	engine.DataDog().StartWorkSpan("test")
	engine.DataDog().StartAPM("test_service", "test")
	engine.DataDog().StartWorkSpan("test")

	consumer.DisableLoop()
	consumer.SetMaxLoopDuration(time.Millisecond)

	consumed := 0
	consumer.SetHeartBeat(func() {
		consumed++
	})
	consumer.Consume(func(items [][]byte) {
		assert.Len(t, items, 1)
		assert.Equal(t, []byte("hello"), items[0])
	})
	assert.Equal(t, 1, consumed)

	router := engine.GetRabbitMQRouter("test_queue_router_with_ttl")
	consumerRouter := router.NewConsumer("default_consumer")
	consumerRouter.DisableLoop()
	consumerRouter.SetMaxLoopDuration(time.Millisecond)
	consumerRouter.Purge()
	defer consumerRouter.Close()
	router.Publish("", []byte("hello"))
	router.Publish("", []byte("hello2"))

	consumed = 0
	consumerRouter.SetHeartBeat(func() {
		consumed++
	})
	consumerRouter.Consume(func(items [][]byte) {
		assert.Len(t, items, 2)
		assert.Equal(t, []byte("hello"), items[0])
		assert.Equal(t, []byte("hello2"), items[1])
	})
	assert.Equal(t, 1, consumed)
}
