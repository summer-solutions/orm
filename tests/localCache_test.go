package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

func TestGetSetLocal(t *testing.T) {
	registry := &orm.Registry{}
	registry.RegisterLocalCache(10)
	config, err := registry.CreateConfig()
	assert.Nil(t, err)
	engine := config.CreateEngine()

	testLogger := &TestCacheLogger{}
	cache := engine.GetLocalCache()
	cache.RegisterLogger(testLogger)

	val := cache.GetSet("test", 1, func() interface{} {
		return "hello"
	})
	assert.Equal(t, "hello", val)
	assert.Len(t, testLogger.Requests, 2)
	assert.Equal(t, "GET test", testLogger.Requests[0])
	assert.Equal(t, "ADD test", testLogger.Requests[1])

	val = cache.GetSet("test", 1, func() interface{} {
		return "hello"
	})
	assert.Equal(t, "hello", val)
	assert.Len(t, testLogger.Requests, 3)
	assert.Equal(t, "GET test", testLogger.Requests[2])

	cache.HMset("test2", map[string]interface{}{"a": "b", "c": "d"})
	fields := cache.HMget("test2", "a", "c")
	assert.Len(t, fields, 2)
	assert.Equal(t, "b", fields["a"])
	assert.Equal(t, "d", fields["c"])
}
