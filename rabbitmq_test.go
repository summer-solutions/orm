package orm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRabbitMQSimpleQueue(t *testing.T) {
	t.SkipNow()
	registry := &Registry{}
	config := &RabbitMQQueueConfig{Name: "test_queue", Durable: false}
	registry.RegisterRabbitMQQueue(config)
	engine := PrepareTables(t, registry)

	queue := engine.GetRabbitMQQueue("test_queue")

	consumer := queue.NewConsumer("consumer 1")
	consumer.SetMaxLoopDuration(time.Millisecond * 100)
	//consumer.DisableLoop()
	consumer.Purge()

	consumer2 := queue.NewConsumer("consumer 2")
	consumer2.SetMaxLoopDuration(time.Millisecond * 100)
	//consumer2.DisableLoop()

	consumed := false
	consumed2 := false
	go func() {
		consumer.Consume(func(items [][]byte) {
			consumed = true
		})
	}()
	go func() {
		consumer2.Consume(func(items [][]byte) {
			consumed2 = true
		})
	}()
	time.Sleep(time.Second * 2)
	queue.Publish([]byte("Hello"))
	assert.NotNil(t, queue)
	time.Sleep(time.Second * 2)

	consumer.Close()
	consumer2.Close()
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
