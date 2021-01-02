package orm

import (
	"fmt"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestRedisStreamGroupConsumerAck(t *testing.T) {
	testRedisStreamGroupConsumer(t, false)
}

func TestRedisStreamGroupConsumerDelete(t *testing.T) {
	testRedisStreamGroupConsumer(t, true)
}

func testRedisStreamGroupConsumer(t *testing.T, autoDelete bool) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6381", 15)
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	engine := validatedRegistry.CreateEngine()
	r := engine.GetRedis()
	r.FlushDB()

	consumer := r.NewStreamGroupConsumer("test-consumer", "test-group", autoDelete, 5, time.Millisecond, "test-stream")
	consumer.DisableLoop()
	heartBeats := 0
	consumer.SetHeartBeat(time.Second, func() {
		heartBeats++
	})
	consumer.Consume(func(streams []redis.XStream, ack *RedisStreamGroupAck) {})
	assert.Equal(t, 1, heartBeats)

	for i := 1; i <= 10; i++ {
		r.XAdd(&redis.XAddArgs{ID: "*", Stream: "test-stream", Values: []string{"name", fmt.Sprintf("a%d", i)}})
	}
	iterations := 0
	consumer.Consume(func(streams []redis.XStream, ack *RedisStreamGroupAck) {
		iterations++
		assert.Len(t, streams, 1)
		assert.Len(t, streams[0].Messages, 5)
		assert.Equal(t, "test-stream", streams[0].Stream)
		if iterations == 1 {
			assert.Equal(t, "a1", streams[0].Messages[0].Values["name"])
			assert.Equal(t, "a2", streams[0].Messages[1].Values["name"])
			assert.Equal(t, "a3", streams[0].Messages[2].Values["name"])
			assert.Equal(t, "a4", streams[0].Messages[3].Values["name"])
			assert.Equal(t, "a5", streams[0].Messages[4].Values["name"])
		} else {
			assert.Equal(t, "a6", streams[0].Messages[0].Values["name"])
			assert.Equal(t, "a7", streams[0].Messages[1].Values["name"])
			assert.Equal(t, "a8", streams[0].Messages[2].Values["name"])
			assert.Equal(t, "a9", streams[0].Messages[3].Values["name"])
			assert.Equal(t, "a10", streams[0].Messages[4].Values["name"])
		}
		for _, message := range streams[0].Messages {
			ack.Ack(streams[0].Stream, message)
		}
	})
	assert.Equal(t, 2, iterations)
	assert.Equal(t, 2, heartBeats)
	if autoDelete {
		assert.Equal(t, int64(0), r.XLen("test-stream"))
	} else {
		assert.Equal(t, int64(10), r.XLen("test-stream"))
	}
	assert.Equal(t, int64(0), r.XInfoGroups("test-stream")[0].Pending)
	consumer.Consume(func(streams []redis.XStream, ack *RedisStreamGroupAck) {})
	assert.Equal(t, 2, iterations)

	r.XTrim("test-stream", 0, false)
	for i := 11; i <= 20; i++ {
		r.XAdd(&redis.XAddArgs{ID: "*", Stream: "test-stream", Values: []string{"name", fmt.Sprintf("a%d", i)}})
	}
	iterations = 0
	consumer.Consume(func(streams []redis.XStream, ack *RedisStreamGroupAck) {
		iterations++
		assert.Len(t, streams[0].Messages, 5)
		if iterations == 1 {
			assert.Equal(t, "a11", streams[0].Messages[0].Values["name"])
		} else {
			assert.Equal(t, "a16", streams[0].Messages[0].Values["name"])
		}
	})
	assert.Equal(t, 2, iterations)
	assert.Equal(t, int64(10), r.XLen("test-stream"))
	assert.Equal(t, int64(10), r.XInfoGroups("test-stream")[0].Pending)
	iterations = 0
	heartBeats = 0
	consumer.Consume(func(streams []redis.XStream, ack *RedisStreamGroupAck) {
		iterations++
		assert.Len(t, streams[0].Messages, 5)
		if iterations == 1 {
			assert.Equal(t, "a11", streams[0].Messages[0].Values["name"])
		} else {
			assert.Equal(t, "a16", streams[0].Messages[0].Values["name"])
		}
		ack.Ack(streams[0].Stream, streams[0].Messages[0], streams[0].Messages[1])
	})
	assert.Equal(t, 2, iterations)
	assert.Equal(t, 1, heartBeats)
	if autoDelete {
		assert.Equal(t, int64(6), r.XLen("test-stream"))
	} else {
		assert.Equal(t, int64(10), r.XLen("test-stream"))
	}
	assert.Equal(t, int64(6), r.XInfoGroups("test-stream")[0].Pending)

	r.FlushDB()
	for i := 1; i <= 10; i++ {
		r.XAdd(&redis.XAddArgs{ID: "*", Stream: "test-stream", Values: []string{"name", fmt.Sprintf("a%d", i)}})
	}
	consumer = r.NewStreamGroupConsumer("test-consumer", "test-group", autoDelete, 5, time.Millisecond, "test-stream")
	iterations = 0
	consumer.Consume(func(streams []redis.XStream, ack *RedisStreamGroupAck) {
		iterations++
		if iterations == 1 {
			assert.Len(t, streams[0].Messages, 5)
			assert.Equal(t, "a1", streams[0].Messages[0].Values["name"])
			ack.Ack(streams[0].Stream, streams[0].Messages[0], streams[0].Messages[1])
		} else if iterations == 2 {
			assert.Len(t, streams[0].Messages, 5)
			assert.Equal(t, "a6", streams[0].Messages[0].Values["name"])
			ack.Ack(streams[0].Stream, streams[0].Messages[0], streams[0].Messages[1])
		} else if iterations == 3 {
			assert.Len(t, streams[0].Messages, 5)
			assert.Equal(t, "a3", streams[0].Messages[0].Values["name"])
			assert.Equal(t, "a4", streams[0].Messages[1].Values["name"])
			assert.Equal(t, "a5", streams[0].Messages[2].Values["name"])
			assert.Equal(t, "a8", streams[0].Messages[3].Values["name"])
			assert.Equal(t, "a9", streams[0].Messages[4].Values["name"])
			ack.Ack(streams[0].Stream, streams[0].Messages[0], streams[0].Messages[1])
		} else if iterations == 4 {
			assert.Len(t, streams[0].Messages, 1)
			assert.Equal(t, "a10", streams[0].Messages[0].Values["name"])
			ack.Ack(streams[0].Stream, streams[0].Messages[0])
		} else if iterations == 5 {
			assert.Len(t, streams[0].Messages, 3)
			assert.Equal(t, "a5", streams[0].Messages[0].Values["name"])
			assert.Equal(t, "a8", streams[0].Messages[1].Values["name"])
			assert.Equal(t, "a9", streams[0].Messages[2].Values["name"])
			ack.Ack(streams[0].Stream, streams[0].Messages[0], streams[0].Messages[1])
		} else if iterations == 6 {
			assert.Len(t, streams[0].Messages, 1)
			assert.Equal(t, "a9", streams[0].Messages[0].Values["name"])
			ack.Ack(streams[0].Stream, streams[0].Messages[0])
			go func() {
				time.Sleep(time.Millisecond * 100)
				consumer.DisableLoop()
			}()
		}
	})
	assert.Equal(t, 6, iterations)
	r.FlushDB()
	iterations = 0
	consumer = r.NewStreamGroupConsumer("test-consumer-multi", "test-group-multi", autoDelete, 8,
		time.Millisecond, "test-stream-a", "test-stream-b")
	consumer.DisableLoop()
	for i := 1; i <= 10; i++ {
		r.XAdd(&redis.XAddArgs{ID: "*", Stream: "test-stream-a", Values: []string{"name", fmt.Sprintf("a%d", i)}})
		r.XAdd(&redis.XAddArgs{ID: "*", Stream: "test-stream-b", Values: []string{"name", fmt.Sprintf("b%d", i)}})
	}
	consumer.Consume(func(streams []redis.XStream, ack *RedisStreamGroupAck) {
		iterations++
		assert.Len(t, streams, 2)
		if iterations == 1 {
			assert.Len(t, streams[0].Messages, 8)
			assert.Len(t, streams[1].Messages, 8)
		} else {
			assert.Len(t, streams[0].Messages, 2)
			assert.Len(t, streams[1].Messages, 2)
		}
	})
	assert.Equal(t, 2, iterations)
}
