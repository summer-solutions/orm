package tests

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

type TestEntityFlushLazyLocal struct {
	Orm  *orm.ORM `orm:"localCache"`
	ID   uint
	Name string
}

func TestFlushLazyLocal(t *testing.T) {
	var entity TestEntityFlushLazyLocal
	engine := PrepareTables(t, &orm.Registry{}, entity)

	DBLogger := &TestDatabaseLogger{}
	pool, _ := engine.GetMysql()
	pool.RegisterLogger(DBLogger)
	LoggerQueue := &TestCacheLogger{}
	cache, _ := engine.GetRedis("default_queue")
	cache.RegisterLogger(LoggerQueue)

	var entities = make([]interface{}, 10)
	for i := 1; i <= 10; i++ {
		e := TestEntityFlushLazyLocal{Name: "Name " + strconv.Itoa(i)}
		entities[i-1] = &e
	}
	err := engine.FlushLazy(entities...)
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Queries, 0)
	assert.Len(t, LoggerQueue.Requests, 1)
	assert.Equal(t, "LPUSH 1 values lazy_queue", LoggerQueue.Requests[0])

	found, err := engine.TryByID(1, &entity)
	assert.Nil(t, err)
	assert.False(t, found)

	LazyReceiver := orm.NewLazyReceiver(engine, "default")
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
	found, err = engine.TryByID(1, &entity)
	assert.Nil(t, err)
	assert.True(t, found)
	assert.Equal(t, "Name 1", entity.Name)

	DBLogger.Queries = make([]string, 0)
	LoggerQueue.Requests = make([]string, 0)
	entity.Name = "Name 1.1"
	err = engine.FlushLazy(&entity)
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
	found, err = engine.TryByID(1, &entity)
	assert.Nil(t, err)
	assert.True(t, found)
	assert.Equal(t, "Name 1.1", entity.Name)

	DBLogger.Queries = make([]string, 0)
	LoggerQueue.Requests = make([]string, 0)
	entity.Orm.MarkToDelete()
	err = engine.FlushLazy(&entity)
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
	found, err = engine.TryByID(1, &entity)
	assert.Nil(t, err)
	assert.False(t, found)
}
