package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"testing"
)

type TestEntityDirtyQueueAll struct {
	Orm  orm.ORM `orm:"table=TestEntityDirtyQueueAll;mysql=default;dirty=test"`
	Id   uint
	Name string
}

type TestEntityDirtyQueueAge struct {
	Orm  orm.ORM `orm:"table=TestEntityDirtyQueueAge;mysql=default"`
	Id   uint
	Name string
	Age  uint16 `orm:"dirty=test"`
}

func TestDirtyQueue(t *testing.T) {

	entityAll := TestEntityDirtyQueueAll{Name: "Name"}
	entityAge := TestEntityDirtyQueueAge{Name: "Name", Age: 18}
	PrepareTables(entityAll, entityAge)
	orm.RegisterDirtyQueue("test", "default_queue")

	LoggerRedisQueue := TestCacheLogger{}
	orm.GetRedisCache("default_queue").AddLogger(&LoggerRedisQueue)
	orm.GetRedisCache("default_queue").AddLogger(orm.StandardCacheLogger{})

	err := orm.Flush(&entityAll, &entityAge)
	assert.Nil(t, err)
	assert.Len(t, LoggerRedisQueue.Requests, 1)
	assert.Equal(t, "ZADD 2 values test", LoggerRedisQueue.Requests[0])

	entityAll.Name = "Name 2"
	err = orm.Flush(&entityAll)
	assert.Nil(t, err)
	assert.Len(t, LoggerRedisQueue.Requests, 2)
	assert.Equal(t, "ZADD 1 values test", LoggerRedisQueue.Requests[1])

	entityAge.Name = "Name 2"
	err = orm.Flush(&entityAll)
	assert.Nil(t, err)
	assert.Len(t, LoggerRedisQueue.Requests, 2)

	entityAge.Age = 10
	err = orm.Flush(&entityAge)
	assert.Nil(t, err)
	assert.Len(t, LoggerRedisQueue.Requests, 3)
	assert.Equal(t, "ZADD 1 values test", LoggerRedisQueue.Requests[2])
}
