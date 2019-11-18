package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"testing"
)

func TestGetSet(t *testing.T) {

	err := orm.RegisterRedis("localhost:6379", 15).FlushDB()
	assert.Nil(t, err)

	testLogger := TestCacheLogger{}
	orm.GetRedis().AddLogger(&testLogger)

	val, err := orm.GetRedis().GetSet("test", 1, func() interface{} {
		return "hello"
	})
	assert.Nil(t, err)
	assert.Equal(t, "hello", val)
	assert.Len(t, testLogger.Requests, 2)
	assert.Equal(t, "GET test", testLogger.Requests[0])
	assert.Equal(t, "SET [1s] test", testLogger.Requests[1])

}
