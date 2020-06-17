package orm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRabbitMQDelayedQueue(t *testing.T) {
	t.SkipNow()
	registry := &Registry{}
	config := &RabbitMQQueueConfig{Name: "test_delayed", Delayed: true, Durable: true}
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
}
