package tests

import (
	"testing"

	"github.com/apex/log"

	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

type TestEntityFlusherInCacheRedis struct {
	orm.ORM  `orm:"redisCache"`
	ID       uint
	Name     string
	Age      uint16
	IndexAge *orm.CachedQuery `query:":Age = ? ORDER BY :ID"`
}

type TestEntityFlusherInCacheLocal struct {
	orm.ORM
	ID uint
}

func TestFlushInCache(t *testing.T) {
	entityRedis := &TestEntityFlusherInCacheRedis{Name: "Name", Age: 18}
	entityLocal := &TestEntityFlusherInCacheLocal{}
	engine := PrepareTables(t, &orm.Registry{}, entityRedis, entityLocal)

	engine.Track(entityRedis)
	err := engine.Flush()
	assert.Nil(t, err)

	pager := &orm.Pager{CurrentPage: 1, PageSize: 100}
	var rows []*TestEntityFlusherInCacheRedis
	totalRows, err := engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Nil(t, err)
	assert.Equal(t, 1, totalRows)
	assert.Len(t, rows, 1)
	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Nil(t, err)
	assert.Equal(t, 0, totalRows)
	assert.Len(t, rows, 0)

	DBLogger := memory.New()
	pool := engine.GetMysql()
	pool.AddLogger(DBLogger)
	pool.SetLogLevel(log.InfoLevel)
	LoggerRedisCache := memory.New()
	cache := engine.GetRedis()
	cache.AddLogger(LoggerRedisCache)
	cache.SetLogLevel(log.InfoLevel)
	LoggerRedisQueue := memory.New()
	cacheQueue := engine.GetRedis("default_queue")
	cacheQueue.AddLogger(LoggerRedisQueue)
	cacheQueue.SetLogLevel(log.InfoLevel)

	entityRedis.Name = "Name 2"
	entityRedis.Age = 10

	err = engine.FlushInCache(entityLocal, entityRedis)
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Entries, 1)
	assert.Equal(t, "[ORM][MYSQL][EXEC]", DBLogger.Entries[0].Message)
	assert.Len(t, LoggerRedisCache.Entries, 1)
	assert.Equal(t, "[ORM][REDIS][MSET]", LoggerRedisCache.Entries[0].Message)
	assert.Len(t, LoggerRedisQueue.Entries, 1)
	assert.Equal(t, "[ORM][REDIS][SADD]", LoggerRedisQueue.Entries[0].Message)

	var loadedEntity TestEntityFlusherInCacheRedis
	has, err := engine.LoadByID(1, &loadedEntity)
	assert.True(t, has)
	assert.Nil(t, err)
	assert.Equal(t, "Name 2", loadedEntity.Name)
	assert.Len(t, LoggerRedisCache.Entries, 2)
	assert.Equal(t, "[ORM][REDIS][GET]", LoggerRedisCache.Entries[1].Message)

	receiver := orm.NewFlushFromCacheReceiver(engine, "default")
	size, err := receiver.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(1), size)

	has, err = receiver.Digest()
	assert.Nil(t, err)
	assert.True(t, has)
	has, err = receiver.Digest()
	assert.Nil(t, err)
	assert.False(t, has)
	size, err = receiver.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), size)

	var inDB TestEntityFlusherInCacheRedis
	_, err = engine.SearchOne(orm.NewWhere("`ID` = ?", 1), &inDB)
	assert.Nil(t, err)
	assert.Equal(t, "Name 2", inDB.Name)

	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Nil(t, err)
	assert.Equal(t, 0, totalRows)
	assert.Len(t, rows, 0)
	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Nil(t, err)
	assert.Equal(t, 1, totalRows)
	assert.Len(t, rows, 1)
}
