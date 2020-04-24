package tests

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

type TestEntityFlushLazyRedis struct {
	orm.ORM `orm:"redisCache"`
	ID      uint
	Name    string
}

func TestFlushLazyRedis(t *testing.T) {
	var entity TestEntityFlushLazyRedis
	engine := PrepareTables(t, &orm.Registry{}, entity)

	DBLogger := &TestDatabaseLogger{}
	pool := engine.GetMysql()
	pool.RegisterLogger(DBLogger)
	LoggerQueue := &TestCacheLogger{}
	cache := engine.GetRedis("default_queue")
	cache.RegisterLogger(LoggerQueue)

	var entities = make([]interface{}, 10)
	for i := 1; i <= 10; i++ {
		e := &TestEntityFlushLazyRedis{Name: "Name " + strconv.Itoa(i)}
		engine.RegisterEntity(e)
		entities[i-1] = e
		err := e.FlushLazy()
		assert.Nil(t, err)
	}
	assert.Len(t, DBLogger.Queries, 0)
	assert.Len(t, LoggerQueue.Requests, 10)
	assert.Equal(t, "LPUSH 1 values _lazy_queue", LoggerQueue.Requests[0])

	LazyReceiver := orm.NewLazyReceiver(engine, &orm.RedisQueueSenderReceiver{PoolName: "default_queue"})
	size, err := LazyReceiver.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(10), size)
	for i := 1; i <= 10; i++ {
		has, err := LazyReceiver.Digest()
		assert.Nil(t, err)
		assert.True(t, has)
	}
	has, err := LazyReceiver.Digest()
	assert.Nil(t, err)
	assert.False(t, has)
	size, err = LazyReceiver.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), size)
	assert.Len(t, DBLogger.Queries, 10)
	assert.Len(t, LoggerQueue.Requests, 23)
	assert.Equal(t, "RPOP _lazy_queue", LoggerQueue.Requests[20])
	assert.Equal(t, "RPOP _lazy_queue", LoggerQueue.Requests[21])
	found, err := engine.LoadByID(1, &entity)
	assert.Nil(t, err)
	assert.True(t, found)
	assert.Equal(t, "Name 1", entity.Name)

	DBLogger.Queries = make([]string, 0)
	LoggerQueue.Requests = make([]string, 0)
	entity.Name = "Name 1.1"
	err = entity.FlushLazy()
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Queries, 0)
	assert.Len(t, LoggerQueue.Requests, 1)
	assert.Equal(t, "LPUSH 1 values _lazy_queue", LoggerQueue.Requests[0])

	has, err = LazyReceiver.Digest()
	assert.Nil(t, err)
	assert.True(t, has)
	has, err = LazyReceiver.Digest()
	assert.Nil(t, err)
	assert.False(t, has)
	assert.Len(t, DBLogger.Queries, 1)
	assert.Len(t, LoggerQueue.Requests, 3)
	assert.Equal(t, "RPOP _lazy_queue", LoggerQueue.Requests[1])
	assert.Equal(t, "RPOP _lazy_queue", LoggerQueue.Requests[2])
	found, err = engine.LoadByID(1, &entity)
	assert.Nil(t, err)
	assert.True(t, found)
	assert.Equal(t, "Name 1.1", entity.Name)

	DBLogger.Queries = make([]string, 0)
	LoggerQueue.Requests = make([]string, 0)
	entity.MarkToDelete()
	err = entity.FlushLazy()
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Queries, 0)
	assert.Len(t, LoggerQueue.Requests, 1)
	assert.Equal(t, "LPUSH 1 values _lazy_queue", LoggerQueue.Requests[0])

	has, err = LazyReceiver.Digest()
	assert.True(t, has)
	assert.Nil(t, err)
	has, err = LazyReceiver.Digest()
	assert.False(t, has)
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Queries, 1)
	assert.Len(t, LoggerQueue.Requests, 3)
	assert.Equal(t, "RPOP _lazy_queue", LoggerQueue.Requests[1])
	assert.Equal(t, "RPOP _lazy_queue", LoggerQueue.Requests[2])
	found, err = engine.LoadByID(1, &entity)
	assert.Nil(t, err)
	assert.False(t, found)
}
