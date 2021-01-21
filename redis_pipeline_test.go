package orm

import (
	"testing"
	"time"

	apexLog "github.com/apex/log"
	"github.com/apex/log/handlers/memory"
	"github.com/go-redis/redis/v8"
	"github.com/stretchr/testify/assert"
)

func TestRedisPipeline(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6381", 15)
	registry.RegisterRedisStream("test-stream", "default", []string{"test-group"})
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()
	r := engine.GetRedis()
	r.FlushDB()
	pipeLine := r.PipeLine()

	r.Set("a", "A", 10)
	r.Set("c", "C", 10)
	testLogger := memory.New()
	engine.AddQueryLogger(testLogger, apexLog.InfoLevel, QueryLoggerSourceRedis)
	c1 := pipeLine.Get("a")
	c2 := pipeLine.Get("b")
	c3 := pipeLine.Get("c")
	assert.PanicsWithError(t, "pipeline must be executed first", func() {
		_, _, _ = c1.Result()
	})
	assert.False(t, pipeLine.Executed())
	pipeLine.Exec()
	assert.Len(t, testLogger.Entries, 1)
	assert.True(t, pipeLine.Executed())
	assert.PanicsWithError(t, "pipeline is already executed", func() {
		pipeLine.Exec()
	})
	val, has, _ := c1.Result()
	assert.Equal(t, "A", val)
	assert.True(t, has)
	val, has, _ = c2.Result()
	assert.Equal(t, "", val)
	assert.False(t, has)
	val, has, _ = c3.Result()
	assert.Equal(t, "C", val)
	assert.True(t, has)

	pipeLine = r.PipeLine()
	c4 := pipeLine.XAdd("test-stream", []string{"key", "a", "key2", "b"})
	c5 := pipeLine.XAdd("test-stream", []string{"key", "c", "key2", "d"})
	c6 := pipeLine.XAdd("a", []string{"key", "c", "key2", "d"})
	c7 := pipeLine.XAdd("test-stream", []string{"key", "e", "key2", "f"})
	assert.Panics(t, func() {
		pipeLine.Exec()
	})
	val, err = c4.Result()
	assert.NotEmpty(t, val)
	assert.NoError(t, err)
	val, err = c5.Result()
	assert.NotEmpty(t, val)
	assert.NoError(t, err)
	val, err = c6.Result()
	assert.Empty(t, val)
	assert.Error(t, err)
	val, err = c7.Result()
	assert.NotEmpty(t, val)
	assert.NoError(t, err)

	assert.Equal(t, int64(3), r.XLen("test-stream"))
	events := r.XRead(&redis.XReadArgs{Count: 10, Streams: []string{"test-stream", "0"}, Block: time.Millisecond * 500})
	assert.Len(t, events, 1)
	assert.Len(t, events[0].Messages, 3)
}
