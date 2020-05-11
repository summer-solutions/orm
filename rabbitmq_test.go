package orm

import (
	"testing"

	"github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

func TestRabbitMQQueue(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRabbitMQServer("amqp://rabbitmq_user:rabbitmq_password@localhost:5672/")
	registry.RegisterRabbitMQQueue("default", &RabbitMQQueueConfig{Name: "test_queue", AutoDelete: true, PrefetchCount: 2})
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()
	defer engine.Defer()

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
	items, err := consumer.Consume()
	assert.NoError(t, err)
	assert.NotNil(t, items)
	item1 := <-items
	item2 := <-items
	assert.NotNil(t, item1)
	assert.Equal(t, []byte("hello"), item1.Body)
	assert.NotNil(t, item2)
	assert.Equal(t, []byte("hello2"), item2.Body)
	err = item2.Ack(true)
	assert.NoError(t, err)
}

func TestRabbitMQQueueExchange(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRabbitMQServer("amqp://rabbitmq_user:rabbitmq_password@localhost:5672/")
	registry.RegisterRabbitMQQueue("default", &RabbitMQQueueConfig{Name: "test_queue_exchange", AutoDelete: true,
		Exchange: "test_exchange_topic", RouterKeys: []string{"aa", "bb"}})
	registry.RegisterRabbitMQExchange("default", &RabbitMQExchangeConfig{Name: "test_exchange_topic", Type: "topic", AutoDelete: true})
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()
	defer engine.Defer()

	r := engine.GetRabbitMQRouter("test_queue_exchange")
	testLogger := memory.New()
	r.AddLogger(testLogger)
	r.SetLogLevel(log.InfoLevel)

	assert.NotNil(t, r)

	consumer, err := r.NewConsumer("test consumer")
	assert.NoError(t, err)
	items, err := consumer.Consume()
	assert.NoError(t, err)
	assert.NotNil(t, items)

	consumer2, err := r.NewConsumer("test consumer")
	assert.NoError(t, err)
	items2, err := consumer2.Consume()
	assert.NoError(t, err)
	assert.NotNil(t, items)

	err = r.Publish("aa", []byte("hello"))
	assert.NoError(t, err)

	item := <-items
	assert.NotNil(t, item)
	assert.Equal(t, []byte("hello"), item.Body)

	item2 := <-items2
	assert.NotNil(t, item2)
	assert.Equal(t, []byte("hello"), item2.Body)
}
