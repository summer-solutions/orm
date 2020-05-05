package orm

import (
	"strconv"
	"testing"

	"github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

type testEntityFlushLazyRedis struct {
	ORM  `orm:"redisCache"`
	ID   uint
	Name string
}

func TestFlushLazyRedis(t *testing.T) {
	var entity testEntityFlushLazyRedis
	engine := PrepareTables(t, &Registry{}, entity)

	DBLogger := memory.New()
	pool := engine.GetMysql()
	pool.AddLogger(DBLogger)
	LoggerQueue := memory.New()
	pool.SetLogLevel(log.InfoLevel)
	cache := engine.GetRedis("default_queue")
	cache.AddLogger(LoggerQueue)
	cache.SetLogLevel(log.InfoLevel)

	var entities = make([]interface{}, 10)
	for i := 1; i <= 10; i++ {
		e := &testEntityFlushLazyRedis{Name: "Name " + strconv.Itoa(i)}
		engine.Track(e)
		entities[i-1] = e
	}
	err := engine.FlushLazy()
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Entries, 0)
	assert.Len(t, LoggerQueue.Entries, 1)
	assert.Equal(t, "[ORM][REDIS][LPUSH]", LoggerQueue.Entries[0].Message)

	LazyReceiver := NewLazyReceiver(engine)
	val, found, err := engine.GetRedis("default_queue").RPop("default_queue")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, val)
	err = LazyReceiver.Digest([]byte(val))
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Entries, 1)
	assert.Len(t, LoggerQueue.Entries, 3)
	assert.Equal(t, "[ORM][REDIS][RPOP]", LoggerQueue.Entries[1].Message)
	assert.Equal(t, "[ORM][REDIS][RPOP]", LoggerQueue.Entries[2].Message)
	found, err = engine.LoadByID(1, &entity)
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

	val, found, err = engine.GetRedis("default_queue").RPop("default_queue")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, val)
	err = LazyReceiver.Digest([]byte(val))
	assert.Nil(t, err)
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

	val, found, err = engine.GetRedis("default_queue").RPop("default_queue")
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, val)
	err = LazyReceiver.Digest([]byte(val))
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Entries, 1)
	assert.Len(t, LoggerQueue.Entries, 3)
	assert.Equal(t, "[ORM][REDIS][RPOP]", LoggerQueue.Entries[1].Message)
	assert.Equal(t, "[ORM][REDIS][RPOP]", LoggerQueue.Entries[2].Message)
	found, err = engine.LoadByID(1, &entity)
	assert.Nil(t, err)
	assert.False(t, found)
}
