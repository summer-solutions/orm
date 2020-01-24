package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"strconv"
	"testing"
)

type TestEntityFlushLazy struct {
	Orm  *orm.ORM `orm:"redisCache"`
	Id   uint
	Name string
}

func TestFlushLazy(t *testing.T) {
	var entity TestEntityFlushLazy
	PrepareTables(entity)

	DBLogger := &TestDatabaseLogger{}
	orm.GetMysql().RegisterLogger(DBLogger.Logger())
	LoggerQueue := TestCacheLogger{}
	orm.GetRedis("default_queue").AddLogger(&LoggerQueue)

	var entities = make([]interface{}, 10)
	for i := 1; i <= 10; i++ {
		e := TestEntityFlushLazy{Name: "Name " + strconv.Itoa(i)}
		entities[i-1] = &e
	}
	err := orm.FlushLazy(entities...)
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Queries, 0)
	assert.Len(t, LoggerQueue.Requests, 1)
	assert.Equal(t, "LPUSH 1 values lazy_queue", LoggerQueue.Requests[0])

	found, err := orm.TryById(1, &entity)
	assert.Nil(t, err)
	assert.False(t, found)

	LazyReceiver := orm.LazyReceiver{QueueName: "default"}
	size, err := LazyReceiver.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(1), size)
	has, err := LazyReceiver.Digest()
	assert.Nil(t, err)
	assert.True(t, has)
	has, err = LazyReceiver.Digest()
	assert.Nil(t, err)
	assert.False(t, has)
	size, err = LazyReceiver.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), size)
	assert.Len(t, DBLogger.Queries, 2)
	assert.Len(t, LoggerQueue.Requests, 5)
	assert.Equal(t, "RPOP lazy_queue", LoggerQueue.Requests[2])
	assert.Equal(t, "RPOP lazy_queue", LoggerQueue.Requests[3])
	found, err = orm.TryById(1, &entity)
	assert.Nil(t, err)
	assert.True(t, found)
	assert.Equal(t, "Name 1", entity.Name)

	DBLogger.Queries = make([]string, 0)
	LoggerQueue.Requests = make([]string, 0)
	entity.Name = "Name 1.1"
	err = orm.FlushLazy(&entity)
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Queries, 0)
	assert.Len(t, LoggerQueue.Requests, 1)
	assert.Equal(t, "LPUSH 1 values lazy_queue", LoggerQueue.Requests[0])

	has, err = LazyReceiver.Digest()
	assert.Nil(t, err)
	assert.True(t, has)
	has, err = LazyReceiver.Digest()
	assert.Nil(t, err)
	assert.False(t, has)
	assert.Len(t, DBLogger.Queries, 1)
	assert.Len(t, LoggerQueue.Requests, 3)
	assert.Equal(t, "RPOP lazy_queue", LoggerQueue.Requests[1])
	assert.Equal(t, "RPOP lazy_queue", LoggerQueue.Requests[2])
	found, err = orm.TryById(1, &entity)
	assert.Nil(t, err)
	assert.True(t, found)
	assert.Equal(t, "Name 1.1", entity.Name)

	DBLogger.Queries = make([]string, 0)
	LoggerQueue.Requests = make([]string, 0)
	entity.Orm.MarkToDelete()
	err = orm.FlushLazy(&entity)
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Queries, 0)
	assert.Len(t, LoggerQueue.Requests, 1)
	assert.Equal(t, "LPUSH 1 values lazy_queue", LoggerQueue.Requests[0])

	has, err = LazyReceiver.Digest()
	assert.True(t, has)
	assert.Nil(t, err)
	has, err = LazyReceiver.Digest()
	assert.False(t, has)
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Queries, 1)
	assert.Len(t, LoggerQueue.Requests, 3)
	assert.Equal(t, "RPOP lazy_queue", LoggerQueue.Requests[1])
	assert.Equal(t, "RPOP lazy_queue", LoggerQueue.Requests[2])
	found, err = orm.TryById(1, &entity)
	assert.Nil(t, err)
	assert.False(t, found)

}
