package tools

import (
	"testing"

	"github.com/go-redis/redis/v8"
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
	id := engine.GetEventBroker().PublishMap("test-stream", orm.EventAsMap{"a": "b"})
	r.XReadGroup(&redis.XReadGroupArgs{Group: "test-group", Consumer: "test-consumer", Count: 1, Streams: []string{"test-stream", ">"}})

	stats = GetRedisStreamsStatistics(engine)
	assert.Equal(t, uint64(1), stats[0].Len)
	assert.Len(t, stats[0].Groups, 1)
	assert.Equal(t, "test-group", stats[0].Groups[0].Group)
	assert.Equal(t, id, stats[0].Groups[0].Higher)
	assert.Equal(t, id, stats[0].Groups[0].Lower)
	assert.Equal(t, uint64(1), stats[0].Groups[0].Pending)
	assert.Len(t, stats[0].Groups[0].Consumers, 1)
	assert.Equal(t, "test-consumer", stats[0].Groups[0].Consumers[0].Name)
	assert.Equal(t, uint64(1), stats[0].Groups[0].Consumers[0].Pending)
}
