package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apex/log"

	"github.com/apex/log/handlers/memory"

	"github.com/go-redis/redis/v7"
)

func TestRedisEnableDebug(t *testing.T) {
	r := prepareRedis(t)
	r.EnableDebug()
	assert.NotNil(t, r.log)
	assert.Equal(t, log.DebugLevel, r.log.Level)
}

func TestGetSetRedis(t *testing.T) {
	r := prepareRedis(t)
	testLogger := memory.New()
	r.AddLogger(testLogger)
	r.SetLogLevel(log.InfoLevel)

	val, err := r.GetSet("test", 1, func() interface{} {
		return "hello"
	})
	assert.Nil(t, err)
	assert.Equal(t, "hello", val)
	assert.Len(t, testLogger.Entries, 2)
	assert.Equal(t, "[ORM][REDIS][GET]", testLogger.Entries[0].Message)
	assert.Equal(t, "[ORM][REDIS][SET]", testLogger.Entries[1].Message)
	val, err = r.GetSet("test", 1, func() interface{} {
		return "hello"
	})
	assert.Nil(t, err)
	assert.Equal(t, "hello", val)
	assert.Len(t, testLogger.Entries, 3)
	assert.Equal(t, "[ORM][REDIS][GET]", testLogger.Entries[2].Message)
}

func TestList(t *testing.T) {
	r := prepareRedis(t)
	testLogger := memory.New()
	r.AddLogger(testLogger)
	r.SetLogLevel(log.InfoLevel)

	total, err := r.LPush("key", "a", "b", "c")
	assert.Nil(t, err)
	assert.Equal(t, int64(3), total)

	total, err = r.LLen("key")
	assert.Nil(t, err)
	assert.Equal(t, int64(3), total)

	total, err = r.RPush("key", "d")
	assert.Nil(t, err)
	assert.Equal(t, int64(4), total)

	elements, err := r.LRange("key", 0, 1)
	assert.Nil(t, err)
	assert.Equal(t, []string{"c", "b"}, elements)

	err = r.LSet("key", 1, "f")
	assert.Nil(t, err)

	elements, err = r.LRange("key", 0, 1)
	assert.Nil(t, err)
	assert.Equal(t, []string{"c", "f"}, elements)

	err = r.LRem("key", 1, "c")
	assert.Nil(t, err)

	elements, err = r.LRange("key", 0, 5)
	assert.Nil(t, err)
	assert.Equal(t, []string{"f", "a", "d"}, elements)

	element, found, err := r.RPop("key")
	assert.Nil(t, err)
	assert.True(t, found)
	assert.Equal(t, "d", element)

	err = r.Del("key")
	assert.Nil(t, err)

	element, found, err = r.RPop("key")
	assert.Nil(t, err)
	assert.False(t, found)
	assert.Equal(t, "", element)

	total, err = r.LPush("key", "a", "b", "c")
	assert.Nil(t, err)
	assert.Equal(t, int64(3), total)

	err = r.Ltrim("key", 1, 3)
	assert.Nil(t, err)
	elements, err = r.LRange("key", 0, 10)
	assert.Nil(t, err)
	assert.Equal(t, []string{"b", "a"}, elements)
}

func TestHash(t *testing.T) {
	r := prepareRedis(t)
	testLogger := memory.New()
	r.AddLogger(testLogger)
	r.SetLogLevel(log.InfoLevel)

	err := r.HSet("key", "field_1", "a")
	assert.Nil(t, err)

	fields, err := r.HMget("key", "field_1", "field_2")
	assert.Nil(t, err)
	assert.Equal(t, fields["field_1"], "a")
	assert.Nil(t, fields["field_2"])

	err = r.HMset("key", map[string]interface{}{"field_3": "c", "field_4": "d"})
	assert.Nil(t, err)

	fieldsAll, err := r.HGetAll("key")
	assert.Nil(t, err)
	assert.Len(t, fieldsAll, 3)
	assert.Contains(t, fieldsAll, "field_1")
	assert.Contains(t, fieldsAll, "field_3")
	assert.Contains(t, fieldsAll, "field_4")
	assert.Equal(t, "a", fieldsAll["field_1"], "a")
	assert.Equal(t, "c", fieldsAll["field_3"])
	assert.Equal(t, "d", fieldsAll["field_4"])
}

func TestSet(t *testing.T) {
	r := prepareRedis(t)
	testLogger := memory.New()
	r.AddLogger(testLogger)
	r.SetLogLevel(log.InfoLevel)

	total, err := r.ZAdd("key", &redis.Z{Member: "a", Score: 100})
	assert.Nil(t, err)
	assert.Equal(t, int64(1), total)

	total, err = r.ZAdd("key", &redis.Z{Member: "v", Score: 200})
	assert.Nil(t, err)
	assert.Equal(t, int64(1), total)

	total, err = r.ZCard("key")
	assert.Nil(t, err)
	assert.Equal(t, int64(2), total)

	total, err = r.ZCount("key", "100", "150")
	assert.Nil(t, err)
	assert.Equal(t, int64(1), total)
}

func prepareRedis(t *testing.T) *RedisCache {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6379", 15)
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()
	r := engine.GetRedis()
	err = r.FlushDB()
	assert.Nil(t, err)
	return r
}
