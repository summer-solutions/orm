package orm

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestRedisStreamGroupConsumerClean(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6381", 15)
	registry.RegisterLocker("default", "default")
	registry.RegisterRedisStream("test-stream", "default", []string{"test-group-1", "test-group-2"})
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	engine := validatedRegistry.CreateEngine()
	r := engine.GetRedis()
	r.FlushDB()
	for i := 1; i <= 10; i++ {
		r.XAdd("test-stream", []string{"name", fmt.Sprintf("a%d", i)})
	}
	consumer1 := r.NewStreamGroupConsumer("test-consumer", "test-group-1", 1, 1, "test-stream")
	consumer1.(*redisStreamGroupConsumer).block = time.Millisecond
	consumer1.(*redisStreamGroupConsumer).garbageTick = time.Millisecond * 15
	consumer1.(*redisStreamGroupConsumer).garbageLock = time.Millisecond * 15
	consumer1.DisableLoop()
	consumer2 := r.NewStreamGroupConsumer("test-consumer", "test-group-2", 1, 1, "test-stream")
	consumer2.(*redisStreamGroupConsumer).block = time.Millisecond
	consumer2.(*redisStreamGroupConsumer).garbageTick = time.Millisecond * 15
	consumer2.(*redisStreamGroupConsumer).garbageLock = time.Millisecond * 15
	consumer2.DisableLoop()

	consumer1.Consume(context.Background(), func(streams []redis.XStream, ack *RedisStreamGroupAck) {
		ack.Ack(streams[0].Stream, streams[0].Messages...)
		time.Sleep(time.Millisecond * 20)
	})
	assert.Equal(t, int64(10), engine.GetRedis().XLen("test-stream"))

	consumer2.Consume(context.Background(), func(streams []redis.XStream, ack *RedisStreamGroupAck) {
		ack.Ack(streams[0].Stream, streams[0].Messages...)
		time.Sleep(time.Millisecond * 100)
	})
	assert.Equal(t, int64(0), engine.GetRedis().XLen("test-stream"))
}

func TestRedisStreamGroupConsumerAutoScaled(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6381", 15)
	registry.RegisterLocker("default", "default")
	registry.RegisterRedisStream("test-stream", "default", []string{"test-group"})
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	engine := validatedRegistry.CreateEngine()
	r := engine.GetRedis()
	r.FlushDB()

	consumer := r.NewStreamGroupConsumer("test-consumer", "test-group", 1, 2, "test-stream")
	consumer.(*redisStreamGroupConsumer).block = time.Millisecond
	consumer.DisableLoop()
	consumer.Consume(context.Background(), func(streams []redis.XStream, ack *RedisStreamGroupAck) {})
	assert.Equal(t, 1, consumer.(*redisStreamGroupConsumer).nr)
	consumer.Consume(context.Background(), func(streams []redis.XStream, ack *RedisStreamGroupAck) {})
	assert.Equal(t, 1, consumer.(*redisStreamGroupConsumer).nr)

	r.FlushDB()
	for i := 1; i <= 10; i++ {
		r.XAdd("test-stream", []string{"name", fmt.Sprintf("a%d", i)})
	}
	iterations1 := false
	iterations2 := false
	wg := &sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		consumer := r.NewStreamGroupConsumer("test-consumer", "test-group", 5, 2, "test-stream")
		consumer.(*redisStreamGroupConsumer).block = time.Millisecond
		consumer.DisableLoop()
		consumer.Consume(context.Background(), func(streams []redis.XStream, ack *RedisStreamGroupAck) {
			assert.Equal(t, 1, consumer.(*redisStreamGroupConsumer).nr)
			iterations1 = true
			time.Sleep(time.Millisecond * 20)
		})
	}()
	time.Sleep(time.Millisecond)
	go func() {
		defer wg.Done()
		consumer := r.NewStreamGroupConsumer("test-consumer", "test-group", 5, 2, "test-stream")
		consumer.(*redisStreamGroupConsumer).block = time.Millisecond
		consumer.DisableLoop()
		consumer.Consume(context.Background(), func(streams []redis.XStream, ack *RedisStreamGroupAck) {
			assert.Equal(t, 2, consumer.(*redisStreamGroupConsumer).nr)
			iterations2 = true
			time.Sleep(time.Millisecond * 20)
		})
	}()
	wg.Wait()
	assert.True(t, iterations1)
	assert.True(t, iterations2)

	pending := r.XPending("test-stream", "test-group")
	assert.Len(t, pending.Consumers, 2)
	assert.NotEmpty(t, pending.Consumers["test-consumer-1"])
	assert.NotEmpty(t, pending.Consumers["test-consumer-2"])

	consumer = r.NewStreamGroupConsumer("test-consumer", "test-group", 100, 2, "test-stream")
	consumer.(*redisStreamGroupConsumer).block = time.Millisecond
	consumer.DisableLoop()
	consumer.(*redisStreamGroupConsumer).minIdle = time.Millisecond
	time.Sleep(time.Millisecond * 100)
	consumer.Consume(context.Background(), func(streams []redis.XStream, ack *RedisStreamGroupAck) {})

	pending = r.XPending("test-stream", "test-group")
	assert.Len(t, pending.Consumers, 1)
	assert.Equal(t, int64(10), pending.Consumers["test-consumer-1"])
}

func TestRedisStreamGroupConsumer(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6381", 15)
	registry.RegisterLocker("default", "default")
	registry.RegisterRedisStream("test-stream", "default", []string{"test-group"})
	registry.RegisterRedisStream("test-stream-a", "default", []string{"test-group", "test-group-multi"})
	registry.RegisterRedisStream("test-stream-b", "default", []string{"test-group", "test-group-multi"})
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	engine := validatedRegistry.CreateEngine()
	r := engine.GetRedis()
	r.FlushDB()

	consumer := r.NewStreamGroupConsumer("test-consumer", "test-group", 5, 1, "test-stream")

	consumer.(*redisStreamGroupConsumer).block = time.Millisecond
	consumer.DisableLoop()
	heartBeats := 0
	consumer.SetHeartBeat(time.Second, func() {
		heartBeats++
	})
	ctx, cancel := context.WithCancel(context.Background())
	consumer.Consume(ctx, func(streams []redis.XStream, ack *RedisStreamGroupAck) {})
	assert.Equal(t, 1, heartBeats)

	for i := 1; i <= 10; i++ {
		r.XAdd("test-stream", []string{"name", fmt.Sprintf("a%d", i)})
	}
	iterations := 0
	consumer.Consume(ctx, func(streams []redis.XStream, ack *RedisStreamGroupAck) {
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
	assert.Equal(t, int64(10), r.XLen("test-stream"))
	assert.Equal(t, int64(0), r.XInfoGroups("test-stream")[0].Pending)
	consumer.Consume(ctx, func(streams []redis.XStream, ack *RedisStreamGroupAck) {})
	assert.Equal(t, 2, iterations)

	r.XTrim("test-stream", 0, false)
	for i := 11; i <= 20; i++ {
		r.XAdd("test-stream", []string{"name", fmt.Sprintf("a%d", i)})
	}
	iterations = 0
	consumer.Consume(ctx, func(streams []redis.XStream, ack *RedisStreamGroupAck) {
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
	consumer.Consume(ctx, func(streams []redis.XStream, ack *RedisStreamGroupAck) {
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
	assert.Equal(t, int64(10), r.XLen("test-stream"))
	assert.Equal(t, int64(6), r.XInfoGroups("test-stream")[0].Pending)

	r.FlushDB()
	for i := 1; i <= 10; i++ {
		r.XAdd("test-stream", []string{"name", fmt.Sprintf("a%d", i)})
	}
	consumer = r.NewStreamGroupConsumer("test-consumer", "test-group", 5, 1, "test-stream")
	consumer.(*redisStreamGroupConsumer).block = time.Millisecond
	iterations = 0
	consumer.Consume(ctx, func(streams []redis.XStream, ack *RedisStreamGroupAck) {
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
	consumer = r.NewStreamGroupConsumer("test-consumer-multi", "test-group-multi", 8, 1,
		"test-stream-a", "test-stream-b")
	consumer.(*redisStreamGroupConsumer).block = time.Millisecond
	consumer.DisableLoop()
	for i := 1; i <= 10; i++ {
		r.XAdd("test-stream-a", []string{"name", fmt.Sprintf("a%d", i)})
		r.XAdd("test-stream-b", []string{"name", fmt.Sprintf("b%d", i)})
	}
	consumer.Consume(ctx, func(streams []redis.XStream, ack *RedisStreamGroupAck) {
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

	r.FlushDB()
	iterations = 0
	messages := 0
	valid := false
	consumer = r.NewStreamGroupConsumer("test-consumer-unique", "test-group", 8, 1,
		"test-stream")
	for i := 1; i <= 10; i++ {
		r.XAdd("test-stream", []string{"name", fmt.Sprintf("a%d", i)})
	}
	go func() {
		consumer = r.NewStreamGroupConsumer("test-consumer-unique", "test-group", 8, 1,
			"test-stream")
		consumer.DisableLoop()
		consumer.(*redisStreamGroupConsumer).block = time.Millisecond * 10
		consumer.Consume(ctx, func(streams []redis.XStream, ack *RedisStreamGroupAck) {
			iterations++
			messages += len(streams[0].Messages)
		})
	}()
	time.Sleep(time.Millisecond)
	go func() {
		consumer = r.NewStreamGroupConsumer("test-consumer-unique", "test-group", 8, 1,
			"test-stream")
		consumer.(*redisStreamGroupConsumer).block = time.Millisecond * 10
		assert.PanicsWithError(t, "consumer test-consumer-unique for group test-group is running already", func() {
			valid = true
			consumer.Consume(ctx, func(streams []redis.XStream, ack *RedisStreamGroupAck) {
			})
		})
	}()
	time.Sleep(time.Millisecond * 100)
	assert.Equal(t, 2, iterations)
	assert.Equal(t, 10, messages)
	assert.True(t, valid)

	for i := 1; i <= 10; i++ {
		r.XAdd("test-stream", []string{"name", fmt.Sprintf("a%d", i)})
	}
	valid = false
	consumer = r.NewStreamGroupConsumer("test-consumer-unique", "test-group", 1, 1,
		"test-stream")
	consumer.(*redisStreamGroupConsumer).lockTTL = time.Millisecond * 100
	consumer.(*redisStreamGroupConsumer).lockTick = time.Millisecond * 100
	consumer.(*redisStreamGroupConsumer).block = time.Millisecond * 100
	consumer.DisableLoop()
	assert.PanicsWithError(t, "consumer test-consumer-unique for group test-group lost lock", func() { // TODO
		consumer.Consume(ctx, func(streams []redis.XStream, ack *RedisStreamGroupAck) {
			valid = true
			time.Sleep(time.Millisecond * 500)
		})
	})
	assert.True(t, valid)

	r.FlushDB()
	consumer = r.NewStreamGroupConsumer("test-consumer-unique", "test-group", 1, 1,
		"test-stream")
	consumer.(*redisStreamGroupConsumer).block = time.Millisecond * 400
	valid = true
	go func() {
		time.Sleep(time.Millisecond * 200)
		cancel()
	}()
	consumer.Consume(ctx, func(streams []redis.XStream, ack *RedisStreamGroupAck) {
		valid = false
	})
	assert.True(t, valid)
}
