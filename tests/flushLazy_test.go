package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"strconv"
	"testing"
)

const TestEntityFlushLazyName = "tests.TestEntityFlushLazy"

type TestEntityFlushLazy struct {
	Orm  orm.ORM `orm:"table=TestFlushLazy;mysql=default"`
	Id   uint
	Name string
}

func TestFlushLazy(t *testing.T) {
	var entity TestEntityFlushLazy
	PrepareTables(entity)

	LoggerDB := TestDatabaseLogger{}
	orm.GetMysqlDB("default").AddLogger(&LoggerDB)
	LoggerQueue := TestCacheLogger{}
	orm.GetRedisCache("default_queue").AddLogger(&LoggerQueue)

	orm.GetMysqlDB("default").AddLogger(orm.StandardDatabaseLogger{})
	orm.GetRedisCache("default_queue").AddLogger(orm.StandardCacheLogger{})

	var entities = make([]interface{}, 10)
	for i := 1; i <= 10; i++ {
		e := TestEntityFlushLazy{Name: "Name " + strconv.Itoa(i)}
		entities[i-1] = &e
	}
	err := orm.FlushLazy(entities...)
	assert.Nil(t, err)
	assert.Len(t, LoggerDB.Queries, 0)
	assert.Len(t, LoggerQueue.Requests, 1)
	assert.Equal(t, "LPUSH 1 values lazy_queue", LoggerQueue.Requests[0])

	_, found := orm.TryById(1, TestEntityFlushLazyName)
	assert.False(t, found)

	LazyReceiver := orm.LazyReceiver{RedisName: "default_queue"}
	LazyReceiver.Digest()
	assert.Len(t, LoggerDB.Queries, 2)
	assert.Len(t, LoggerQueue.Requests, 3)
	assert.Equal(t, "RPOP lazy_queue", LoggerQueue.Requests[1])
	assert.Equal(t, "RPOP lazy_queue", LoggerQueue.Requests[2])
	addedEntity, found := orm.TryById(1, TestEntityFlushLazyName)
	assert.True(t, found)
	assert.Equal(t, "Name 1", addedEntity.(TestEntityFlushLazy).Name)

	//TODO EDIT
}
