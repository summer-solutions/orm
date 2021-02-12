package tools

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

func TestRedisStreamsStatus(t *testing.T) {
	registry := &orm.Registry{}
	registry.RegisterRedis("localhost:6381", 15)
	registry.RegisterLocker("default", "default")
	registry.RegisterRedisStream("test-stream", "default", []string{"test-group"})
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	engine := validatedRegistry.CreateEngine()
	r := engine.GetRedis()
	r.FlushDB()

	stats := GetRedisStreamsStatistics(engine)
	assert.Len(t, stats, 1)
	assert.Equal(t, "test-stream", stats[0].Stream)
	assert.Equal(t, "default", stats[0].RedisPool)
	assert.Equal(t, uint64(0), stats[0].Len)
	assert.Len(t, stats[0].Groups, 0)

	r.XGroupCreateMkStream("test-stream", "test-group", "0")
	flusher := engine.GetEventBroker().NewFlusher()
	for i := 1; i <= 10001; i++ {
		flusher.PublishMap("test-stream", orm.EventAsMap{"a": "b"})
	}
	flusher.Flush()
	time.Sleep(time.Millisecond * 300)

	stats = GetRedisStreamsStatistics(engine)
	assert.Equal(t, uint64(10001), stats[0].Len)
	assert.Len(t, stats[0].Groups, 1)
	assert.Equal(t, "test-group", stats[0].Groups[0].Group)
	assert.Equal(t, uint64(0), stats[0].Groups[0].Pending)
	assert.Len(t, stats[0].Groups[0].Consumers, 0)

	consumer := engine.GetEventBroker().Consumer("test-consumer", "test-group")
	consumer.DisableLoop()
	consumer.Consume(context.Background(), 11000, false, func(events []orm.Event) {
		for _, event := range events {
			event.Skip()
		}
		time.Sleep(time.Millisecond * 100)
	})

	stats = GetRedisStreamsStatistics(engine)
	assert.Equal(t, uint64(10001), stats[0].Len)
	assert.Len(t, stats[0].Groups, 1)
	assert.Equal(t, "test-group", stats[0].Groups[0].Group)
	assert.Equal(t, uint64(10001), stats[0].Groups[0].Pending)
	assert.Len(t, stats[0].Groups[0].Consumers, 1)
	assert.Equal(t, "test-consumer-1", stats[0].Groups[0].Consumers[0].Name)
	assert.Equal(t, uint64(10001), stats[0].Groups[0].Consumers[0].Pending)
	assert.Equal(t, int64(10001), stats[0].Groups[0].SpeedEvents)
	assert.GreaterOrEqual(t, stats[0].Groups[0].SpeedMilliseconds, 0.01)
	assert.LessOrEqual(t, stats[0].Groups[0].SpeedMilliseconds, 0.011)
}
