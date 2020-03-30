package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"testing"
)

func TestGetSetRedis(t *testing.T) {
	config := &orm.Config{}
	engine := orm.NewEngine(config)
	config.RegisterRedis("localhost:6379", 15)
	err := engine.GetRedis().FlushDB()
	assert.Nil(t, err)

	testLogger := &TestCacheLogger{}
	engine.GetRedis().RegisterLogger(testLogger.Logger())

	val, err := engine.GetRedis().GetSet("test", 1, func() interface{} {
		return "hello"
	})
	assert.Nil(t, err)
	assert.Equal(t, "hello", val)
	assert.Len(t, testLogger.Requests, 2)
	assert.Equal(t, "GET test", testLogger.Requests[0])
	assert.Equal(t, "SET [1s] test", testLogger.Requests[1])

}
