package orm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRabbitMQRouterQueue(t *testing.T) {
	t.SkipNow()
	registry := &Registry{}
	config := &RabbitMQQueueConfig{Name: "test_router_channel", Durable: false, Router: "test_router"}
	registry.RegisterRabbitMQQueue(config)
	configRouter := &RabbitMQRouterConfig{Name: "test_router", Durable: false, Type: "fanout"}
	registry.RegisterRabbitMQRouter(configRouter)
	engine := PrepareTables(t, registry)

	queue := engine.GetRabbitMQRouter("test_router_channel")
	consumer1 := queue.NewConsumer("consumer 1")
	consumer1.SetMaxLoopDuration(time.Millisecond * 100)
	//consumer1.DisableLoop()
	consumer1.Purge()
	consumer2 := queue.NewConsumer("consumer 2")
	consumer2.SetMaxLoopDuration(time.Millisecond * 100)
	//consumer2.DisableLoop()

	go func() {
		consumer1.Consume(func(items [][]byte) {
		})
	}()
	go func() {
		consumer2.Consume(func(items [][]byte) {
		})
	}()
	time.Sleep(time.Second * 2)
	queue.Publish("", []byte("Hello"))
	queue.Publish("", []byte("Hello2"))
	queue.Publish("", []byte("Hello2"))

	time.Sleep(time.Second * 5)
}

func TestRabbitMQDelayedQueue(t *testing.T) {
	t.SkipNow()
	registry := &Registry{}
	config := &RabbitMQQueueConfig{Name: "test_delayed", Delayed: true, Durable: false}
	registry.RegisterRabbitMQQueue(config)
	engine := PrepareTables(t, registry)

	queue := engine.GetRabbitMQDelayedQueue("test_delayed")

	consumer := queue.NewConsumer("consumer")
	consumer.SetMaxLoopDuration(time.Millisecond * 100)
	consumer.DisableLoop()
	consumer.Purge()

	queue.Publish(time.Millisecond*200, []byte("Hello"))
	assert.NotNil(t, queue)

	consumed := false
	consumer.Consume(func(items [][]byte) {
		consumed = true
	})
	assert.False(t, consumed)
	consumer.Close()

	time.Sleep(time.Millisecond * 300)
	consumer = queue.NewConsumer("consumer")
	consumer.DisableLoop()
	consumer.Consume(func(items [][]byte) {
		consumed = true
	})
	assert.True(t, consumed)
	consumer.Close()
}
