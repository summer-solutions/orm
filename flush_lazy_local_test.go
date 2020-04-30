package orm

import (
	"strconv"
	"testing"

	"github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

type testEntityFlushLazyLocal struct {
	ORM  `orm:"localCache"`
	ID   uint
	Name string
}

func TestFlushLazyLocal(t *testing.T) {
	var entity testEntityFlushLazyLocal
	engine := PrepareTables(t, &Registry{}, entity)

	DBLogger := memory.New()
	pool := engine.GetMysql()
	pool.AddLogger(DBLogger)
	pool.SetLogLevel(log.InfoLevel)
	LoggerQueue := memory.New()
	cache := engine.GetRedis("default_queue")
	cache.AddLogger(LoggerQueue)
	cache.SetLogLevel(log.InfoLevel)

	var entities = make([]interface{}, 10)
	for i := 1; i <= 10; i++ {
		e := &testEntityFlushLazyLocal{Name: "Name " + strconv.Itoa(i)}
		entities[i-1] = e
		engine.Track(e)
	}
	err := engine.FlushLazy()
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Entries, 0)
	assert.Len(t, LoggerQueue.Entries, 1)
	assert.Equal(t, "[ORM][REDIS][LPUSH]", LoggerQueue.Entries[0].Message)

	LazyReceiver := NewLazyReceiver(engine, &RedisQueueSenderReceiver{PoolName: "default_queue"})
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
	assert.Len(t, DBLogger.Entries, 1)
	assert.Len(t, LoggerQueue.Entries, 5)
	assert.Equal(t, "[ORM][REDIS][RPOP]", LoggerQueue.Entries[2].Message)
	assert.Equal(t, "[ORM][REDIS][RPOP]", LoggerQueue.Entries[3].Message)
	found, err := engine.LoadByID(1, &entity)
	assert.Nil(t, err)
	assert.True(t, found)
	assert.Equal(t, "Name 1", entity.Name)

	DBLogger.Entries = make([]*log.Entry, 0)
	LoggerQueue.Entries = make([]*log.Entry, 0)
	engine.Track(&entity)
	entity.Name = "Name 1.1"
	err = engine.FlushLazy()
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Entries, 0)
	assert.Len(t, LoggerQueue.Entries, 1)
	assert.Equal(t, "[ORM][REDIS][LPUSH]", LoggerQueue.Entries[0].Message)

	has, err = LazyReceiver.Digest()
	assert.Nil(t, err)
	assert.True(t, has)
	has, err = LazyReceiver.Digest()
	assert.Nil(t, err)
	assert.False(t, has)
	assert.Len(t, DBLogger.Entries, 1)
	assert.Len(t, LoggerQueue.Entries, 3)
	assert.Equal(t, "[ORM][REDIS][RPOP]", LoggerQueue.Entries[1].Message)
	assert.Equal(t, "[ORM][REDIS][RPOP]", LoggerQueue.Entries[2].Message)
	found, err = engine.LoadByID(1, &entity)
	assert.Nil(t, err)
	assert.True(t, found)
	assert.Equal(t, "Name 1.1", entity.Name)

	DBLogger.Entries = make([]*log.Entry, 0)
	LoggerQueue.Entries = make([]*log.Entry, 0)
	engine.MarkToDelete(&entity)
	err = engine.FlushLazy()
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Entries, 0)
	assert.Len(t, LoggerQueue.Entries, 1)
	assert.Equal(t, "[ORM][REDIS][LPUSH]", LoggerQueue.Entries[0].Message)

	has, err = LazyReceiver.Digest()
	assert.True(t, has)
	assert.Nil(t, err)
	has, err = LazyReceiver.Digest()
	assert.False(t, has)
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Entries, 1)
	assert.Len(t, LoggerQueue.Entries, 3)
	assert.Equal(t, "[ORM][REDIS][RPOP]", LoggerQueue.Entries[1].Message)
	assert.Equal(t, "[ORM][REDIS][RPOP]", LoggerQueue.Entries[2].Message)
	found, err = engine.LoadByID(1, &entity)
	assert.Nil(t, err)
	assert.False(t, found)
}
