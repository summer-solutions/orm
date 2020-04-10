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
	engine := orm.NewEngine(config)

	testLogger := &TestCacheLogger{}
	cache, has := engine.GetLocalCache()
	assert.True(t, has)
	cache.RegisterLogger(testLogger.Logger())

	val := cache.GetSet("test", 1, func() interface{} {
		return "hello"
	})
	assert.Equal(t, "hello", val)
	assert.Len(t, testLogger.Requests, 2)
	assert.Equal(t, "GET test", testLogger.Requests[0])
	assert.Equal(t, "ADD test", testLogger.Requests[1])

}
