package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"testing"
)

func TestGetSetRedis(t *testing.T) {
	config := &orm.Config{}
	config.RegisterRedis("localhost:6379", 15)
	engine := orm.NewEngine(config)
	redis, has := engine.GetRedis()
	assert.True(t, has)
	err := redis.FlushDB()
	assert.Nil(t, err)

	testLogger := &TestCacheLogger{}
	redis.RegisterLogger(testLogger.Logger())

	val, err := redis.GetSet("test", 1, func() interface{} {
		return "hello"
	})
	assert.Nil(t, err)
	assert.Equal(t, "hello", val)
	assert.Len(t, testLogger.Requests, 2)
	assert.Equal(t, "GET test", testLogger.Requests[0])
	assert.Equal(t, "SET [1s] test", testLogger.Requests[1])

}
