package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"strconv"
	"testing"
)

const TestEntityFlushLazyName = "tests.TestEntityFlushLazy"

type TestEntityFlushLazy struct {
	Orm  orm.ORM `orm:"table=TestFlushLazy;mysql=default;redisCache"`
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
	assert.Equal(t, int64(1), LazyReceiver.Size())
	err = LazyReceiver.Digest()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), LazyReceiver.Size())
	assert.Len(t, LoggerDB.Queries, 2)
	assert.Len(t, LoggerQueue.Requests, 5)
	assert.Equal(t, "RPOP lazy_queue", LoggerQueue.Requests[2])
	assert.Equal(t, "RPOP lazy_queue", LoggerQueue.Requests[3])
	addedEntity, found := orm.TryById(1, TestEntityFlushLazyName)
	assert.True(t, found)
	assert.Equal(t, "Name 1", addedEntity.(TestEntityFlushLazy).Name)

	LoggerDB.Queries = make([]string, 0)
	LoggerQueue.Requests = make([]string, 0)
	editedEntity := addedEntity.(TestEntityFlushLazy)
	editedEntity.Name = "Name 1.1"
	err = orm.FlushLazy(&editedEntity)
	assert.Nil(t, err)
	assert.Len(t, LoggerDB.Queries, 0)
	assert.Len(t, LoggerQueue.Requests, 1)
	assert.Equal(t, "LPUSH 1 values lazy_queue", LoggerQueue.Requests[0])

	err = LazyReceiver.Digest()
	assert.Nil(t, err)
	assert.Len(t, LoggerDB.Queries, 1)
	assert.Len(t, LoggerQueue.Requests, 3)
	assert.Equal(t, "RPOP lazy_queue", LoggerQueue.Requests[1])
	assert.Equal(t, "RPOP lazy_queue", LoggerQueue.Requests[2])
	addedEntity, found = orm.TryById(1, TestEntityFlushLazyName)
	assert.True(t, found)
	assert.Equal(t, "Name 1.1", addedEntity.(TestEntityFlushLazy).Name)

	editedEntity = addedEntity.(TestEntityFlushLazy)
	LoggerDB.Queries = make([]string, 0)
	LoggerQueue.Requests = make([]string, 0)
	orm.MarkToDelete(editedEntity)
	err = orm.FlushLazy(&editedEntity)
	assert.Nil(t, err)
	assert.Len(t, LoggerDB.Queries, 0)
	assert.Len(t, LoggerQueue.Requests, 1)
	assert.Equal(t, "LPUSH 1 values lazy_queue", LoggerQueue.Requests[0])

	err = LazyReceiver.Digest()
	assert.Nil(t, err)
	assert.Len(t, LoggerDB.Queries, 1)
	assert.Len(t, LoggerQueue.Requests, 3)
	assert.Equal(t, "RPOP lazy_queue", LoggerQueue.Requests[1])
	assert.Equal(t, "RPOP lazy_queue", LoggerQueue.Requests[2])
	addedEntity, found = orm.TryById(1, TestEntityFlushLazyName)
	assert.False(t, found)

}
