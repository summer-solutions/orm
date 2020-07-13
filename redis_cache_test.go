package orm

import (
	"testing"

	"github.com/go-redis/redis/v7"

	apexLog "github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/go-redis/redis_rate/v8"

	"github.com/stretchr/testify/assert"
)

func TestRedis(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6380", 15)
	registry.RegisterRabbitMQServer("amqp://rabbitmq_user:rabbitmq_password@localhost:5677/test")
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	testRedis(t, validatedRegistry.CreateEngine())
}

func TestRedisRing(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedisRing([]string{"localhost:6380"}, 15)
	registry.RegisterRabbitMQServer("amqp://rabbitmq_user:rabbitmq_password@localhost:5677/test")
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	testRedis(t, validatedRegistry.CreateEngine())
}

func testRedis(t *testing.T, engine *Engine) {
	r := engine.GetRedis()

	testLogger := memory.New()
	engine.AddQueryLogger(testLogger, apexLog.InfoLevel, QueryLoggerSourceRedis)
	r.FlushDB()
	testLogger.Entries = make([]*apexLog.Entry, 0)

	assert.True(t, r.RateLimit("test", redis_rate.PerSecond(2)))
	assert.True(t, r.RateLimit("test", redis_rate.PerSecond(2)))
	assert.False(t, r.RateLimit("test", redis_rate.PerSecond(2)))
	assert.Len(t, testLogger.Entries, 3)

	valid := false
	val := r.GetSet("test_get_set", 10, func() interface{} {
		valid = true
		return "ok"
	})
	assert.True(t, valid)
	assert.Equal(t, "ok", val)
	valid = false
	val = r.GetSet("test_get_set", 10, func() interface{} {
		valid = true
		return "ok"
	})
	assert.False(t, valid)
	assert.Equal(t, "ok", val)

	val, has := r.Get("test_get")
	assert.False(t, has)
	assert.Equal(t, "", val)
	r.Set("test_get", "hello", 1)
	val, has = r.Get("test_get")
	assert.True(t, has)
	assert.Equal(t, "hello", val)

	r.LPush("test_list", "a")
	assert.Equal(t, int64(1), r.LLen("test_list"))
	r.RPush("test_list", "b", "c")
	assert.Equal(t, int64(3), r.LLen("test_list"))
	assert.Equal(t, []string{"a", "b", "c"}, r.LRange("test_list", 0, 2))
	assert.Equal(t, []string{"b", "c"}, r.LRange("test_list", 1, 5))
	r.LSet("test_list", 1, "d")
	assert.Equal(t, []string{"a", "d", "c"}, r.LRange("test_list", 0, 2))
	r.LRem("test_list", 1, "c")
	assert.Equal(t, []string{"a", "d"}, r.LRange("test_list", 0, 2))

	val, has = r.RPop("test_list")
	assert.True(t, has)
	assert.Equal(t, "d", val)
	r.Ltrim("test_list", 1, 2)
	val, has = r.RPop("test_list")
	assert.False(t, has)
	assert.Equal(t, "", val)

	r.HSet("test_map", "name", "Tom")
	assert.Equal(t, map[string]string{"name": "Tom"}, r.HGetAll("test_map"))
	r.HMset("test_map", map[string]interface{}{"last": "Summer", "age": "16"})
	assert.Equal(t, map[string]string{"age": "16", "last": "Summer", "name": "Tom"}, r.HGetAll("test_map"))
	assert.Equal(t, map[string]interface{}{"age": "16", "missing": nil, "name": "Tom"}, r.HMget("test_map",
		"name", "age", "missing"))

	added := r.ZAdd("test_z", &redis.Z{Member: "a", Score: 10}, &redis.Z{Member: "b", Score: 20})
	assert.Equal(t, int64(2), added)
	assert.Equal(t, int64(2), r.ZCard("test_z"))
	assert.Equal(t, int64(2), r.ZCount("test_z", "10", "20"))
	assert.Equal(t, int64(1), r.ZCount("test_z", "11", "20"))
	r.Del("test_z")
	assert.Equal(t, int64(0), r.ZCount("test_z", "10", "20"))

	r.MSet("key_1", "a", "key_2", "b")
	assert.Equal(t, map[string]interface{}{"key_1": "a", "key_2": "b", "missing": nil}, r.MGet("key_1", "key_2", "missing"))

}
