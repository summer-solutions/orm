package orm

import (
	"testing"

	"github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

func TestRabbitMQQueue(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRabbitMQServer("amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test")
	registry.RegisterRabbitMQQueue(&RabbitMQQueueConfig{Name: "test_queue", PrefetchCount: 2, AutoDelete: true})
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()

	r := engine.GetRabbitMQQueue("test_queue")
	testLogger := memory.New()
	engine.AddQueryLogger(testLogger, log.InfoLevel, QueryLoggerSourceRabbitMQ)

	assert.NotNil(t, r)
	r.Publish([]byte("hello"))
	r.Publish([]byte("hello2"))

	consumer := r.NewConsumer("test consumer")
	defer consumer.Close()
	consumer.DisableLoop()
	has := false
	consumer.Consume(func(items [][]byte) {
		has = true
		assert.Len(t, items, 2)
		assert.Equal(t, []byte("hello"), items[0])
		assert.Equal(t, []byte("hello2"), items[1])
	})
	assert.True(t, has)
}

func TestRabbitMQQueueRouter(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRabbitMQServer("amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test")
	registry.RegisterRabbitMQQueue(&RabbitMQQueueConfig{Name: "test_queue_router", AutoDelete: false,
		Router: "test_router_topic", RouterKeys: []string{"aa", "bb"}})
	registry.RegisterRabbitMQRouter(&RabbitMQRouterConfig{Name: "test_router_topic", Type: "topic"})
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()
	r := engine.GetRabbitMQRouter("test_queue_router")
	testLogger := memory.New()
	engine.AddQueryLogger(testLogger, log.InfoLevel, QueryLoggerSourceRabbitMQ)

	consumer := r.NewConsumer("test consumer 1")
	consumer.DisableLoop()
	go func() {
		c := 0
		consumer.Consume(func(items [][]byte) {
			assert.Len(t, items, 1)
			if c == 0 {
				assert.Equal(t, []byte("hello"), items[0])
			} else {
				assert.Equal(t, []byte("hello2"), items[0])
			}
			c++
		})
	}()

	assert.NotNil(t, r)
	r.Publish("aa", []byte("hello"))

	r.Publish("bb", []byte("hello2"))
}
