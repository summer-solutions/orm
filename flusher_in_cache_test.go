package orm

import (
	"testing"

	"github.com/apex/log"

	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

type testEntityFlusherInCacheRedis struct {
	ORM      `orm:"redisCache"`
	ID       uint
	Name     string
	Age      uint16       `orm:"index=AgeIndex"`
	IndexAge *CachedQuery `query:":Age = ?"`
}

type testEntityFlusherInCacheLocal struct {
	ORM
	ID uint
}

func TestFlushInCache(t *testing.T) {
	entityRedis := &testEntityFlusherInCacheRedis{Name: "Name", Age: 18}
	entityLocal := &testEntityFlusherInCacheLocal{}
	engine := PrepareTables(t, &Registry{}, entityRedis, entityLocal)

	engine.Track(entityRedis)
	engine.Flush()

	pager := &Pager{CurrentPage: 1, PageSize: 100}
	var rows []*testEntityFlusherInCacheRedis
	totalRows := engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Equal(t, 1, totalRows)
	assert.Len(t, rows, 1)
	totalRows = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Equal(t, 0, totalRows)
	assert.Len(t, rows, 0)

	DBLogger := memory.New()
	engine.AddQueryLogger(DBLogger, log.InfoLevel, LoggerSourceDB)
	LoggerRedisCache := memory.New()
	engine.AddQueryLogger(LoggerRedisCache, log.InfoLevel, LoggerSourceRedis)

	entityRedis.Name = "Name 2"
	entityRedis.Age = 10

	engine.FlushInCache(entityLocal, entityRedis)
	assert.Len(t, DBLogger.Entries, 1)
	assert.Equal(t, "[ORM][MYSQL][EXEC]", DBLogger.Entries[0].Message)
	assert.Len(t, LoggerRedisCache.Entries, 1)
	assert.Equal(t, "[ORM][REDIS][MSET]", LoggerRedisCache.Entries[0].Message)

	var loadedEntity testEntityFlusherInCacheRedis
	has := engine.LoadByID(1, &loadedEntity)
	assert.True(t, has)
	assert.Equal(t, "Name 2", loadedEntity.Name)
	assert.Len(t, LoggerRedisCache.Entries, 2)
	assert.Equal(t, "[ORM][REDIS][GET]", LoggerRedisCache.Entries[1].Message)

	receiver := NewFlushFromCacheReceiver(engine)
	receiver.DisableLoop()

	receiver.Digest()

	var inDB testEntityFlusherInCacheRedis
	_ = engine.SearchOne(NewWhere("`ID` = ?", 1), &inDB)
	assert.Equal(t, "Name 2", inDB.Name)

	totalRows = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Equal(t, 0, totalRows)
	assert.Len(t, rows, 0)
	totalRows = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Equal(t, 1, totalRows)
	assert.Len(t, rows, 1)
}
