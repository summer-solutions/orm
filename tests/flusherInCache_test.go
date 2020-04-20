package tests

import (
	"testing"

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

	engine.RegisterNewEntity(entityRedis)
	err := entityRedis.Flush()
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

	DBLogger := &TestDatabaseLogger{}
	pool, has := engine.GetMysql()
	assert.True(t, has)
	pool.RegisterLogger(DBLogger)
	LoggerRedisCache := &TestCacheLogger{}
	cache, has := engine.GetRedis()
	assert.True(t, has)
	cache.RegisterLogger(LoggerRedisCache)
	LoggerRedisQueue := &TestCacheLogger{}
	cacheQueue, has := engine.GetRedis("default_queue")
	assert.True(t, has)
	cacheQueue.RegisterLogger(LoggerRedisQueue)

	entityRedis.Name = "Name 2"
	entityRedis.Age = 10

	err = engine.FlushInCache(entityLocal, entityRedis)
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Queries, 1)
	assert.Equal(t, "INSERT INTO TestEntityFlusherInCacheLocal() VALUES () []", DBLogger.Queries[0])
	assert.Len(t, LoggerRedisCache.Requests, 1)
	assert.Equal(t, "MSET [TestEntityFlusherInCacheRedis2048746768:1 ] ", LoggerRedisCache.Requests[0])
	assert.Len(t, LoggerRedisQueue.Requests, 1)
	assert.Equal(t, "SADD 1 values dirty_queue", LoggerRedisQueue.Requests[0])

	var loadedEntity TestEntityFlusherInCacheRedis
	err = engine.GetByID(1, &loadedEntity)
	assert.Nil(t, err)
	assert.Equal(t, "Name 2", loadedEntity.Name)
	assert.Len(t, LoggerRedisCache.Requests, 2)
	assert.Equal(t, "GET TestEntityFlusherInCacheRedis2048746768:1", LoggerRedisCache.Requests[1])

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
