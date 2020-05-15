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
	r.AddLogger(testLogger)
	r.SetLogLevel(log.InfoLevel)

	assert.NotNil(t, r)
	err = r.Publish([]byte("hello"))
	assert.NoError(t, err)
	err = r.Publish([]byte("hello2"))
	assert.NoError(t, err)

	consumer, err := r.NewConsumer("test consumer")
	assert.NoError(t, err)
	defer consumer.Close()
	consumer.DisableLoop()
	has := false
	err = consumer.Consume(func(items [][]byte) error {
		has = true
		assert.Len(t, items, 2)
		assert.Equal(t, []byte("hello"), items[0])
		assert.Equal(t, []byte("hello2"), items[1])
		return nil
	})
	assert.NoError(t, err)
	assert.True(t, has)
}

func TestRabbitMQQueueExchange(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRabbitMQServer("amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test")
	registry.RegisterRabbitMQQueue(&RabbitMQQueueConfig{Name: "test_queue_exchange", AutoDelete: true,
		Router: "test_exchange_topic", RouterKeys: []string{"aa", "bb"}})
	registry.RegisterRabbitMQRouter(&RabbitMQRouterConfig{Name: "test_exchange_topic", Type: "topic"})
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()
	r := engine.GetRabbitMQRouter("test_queue_exchange")
	testLogger := memory.New()
	r.AddLogger(testLogger)
	r.SetLogLevel(log.InfoLevel)

	consumer, err := r.NewConsumer("test consumer 1")
	assert.NoError(t, err)
	consumer.DisableLoop()
	go func() {
		c := 0
		err = consumer.Consume(func(items [][]byte) error {
			assert.Len(t, items, 1)
			if c == 0 {
				assert.Equal(t, []byte("hello"), items[0])
			} else {
				assert.Equal(t, []byte("hello2"), items[0])
			}
			c++
			return nil
		})
		assert.NoError(t, err)
	}()

	assert.NotNil(t, r)
	err = r.Publish("aa", []byte("hello"))
	assert.NoError(t, err)
	err = r.Publish("bb", []byte("hello2"))
	assert.NoError(t, err)
}
