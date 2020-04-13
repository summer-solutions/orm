package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

func TestGetSetRedis(t *testing.T) {
	r := prepareRedis(t)
	testLogger := &TestCacheLogger{}
	r.RegisterLogger(testLogger)

	val, err := r.GetSet("test", 1, func() interface{} {
		return "hello"
	})
	assert.Nil(t, err)
	assert.Equal(t, "hello", val)
	assert.Len(t, testLogger.Requests, 2)
	assert.Equal(t, "GET test", testLogger.Requests[0])
	assert.Equal(t, "SET [1s] test", testLogger.Requests[1])
	val, err = r.GetSet("test", 1, func() interface{} {
		return "hello"
	})
	assert.Nil(t, err)
	assert.Equal(t, "hello", val)
	assert.Len(t, testLogger.Requests, 3)
	assert.Equal(t, "GET test", testLogger.Requests[2])
}

func TestList(t *testing.T) {
	r := prepareRedis(t)
	testLogger := &TestCacheLogger{}
	r.RegisterLogger(testLogger)

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
}

func prepareRedis(t *testing.T) *orm.RedisCache {
	registry := &orm.Registry{}
	registry.RegisterRedis("localhost:6379", 15)
	config, err := registry.CreateConfig()
	assert.Nil(t, err)
	engine := orm.NewEngine(config)
	r, has := engine.GetRedis()
	assert.True(t, has)
	err = r.FlushDB()
	assert.Nil(t, err)
	return r
}
