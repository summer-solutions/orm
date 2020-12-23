package orm

import (
	"testing"

	apexLog "github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

type requestCacheEntity struct {
	ORM       `orm:"redisCache"`
	ID        uint
	Name      string       `orm:"length=100;index=name"`
	IndexName *CachedQuery `query:":Name = ?"`
}

func TestRequestCache(t *testing.T) {
	var entity *requestCacheEntity
	engine := PrepareTables(t, &Registry{}, 5, entity)

	engine.Track(&requestCacheEntity{Name: "a"})
	engine.Track(&requestCacheEntity{Name: "b"})
	engine.Track(&requestCacheEntity{Name: "c"})
	engine.Track(&requestCacheEntity{Name: "d"})
	engine.Track(&requestCacheEntity{Name: "d"})
	engine.Flush()

	engine.EnableRequestCache(false)

	DBLogger := memory.New()
	engine.AddQueryLogger(DBLogger, apexLog.InfoLevel, QueryLoggerSourceDB)
	redisLogger := memory.New()
	engine.AddQueryLogger(redisLogger, apexLog.InfoLevel, QueryLoggerSourceRedis)

	entity = &requestCacheEntity{}
	found := engine.LoadByID(1, entity)
	assert.True(t, found)
	assert.Equal(t, uint(1), entity.ID)
	assert.Equal(t, "a", entity.Name)
	assert.Len(t, DBLogger.Entries, 1)
	assert.Len(t, redisLogger.Entries, 2)

	found = engine.LoadByID(1, entity)
	assert.True(t, found)
	assert.Equal(t, uint(1), entity.ID)
	assert.Equal(t, "a", entity.Name)
	assert.Len(t, DBLogger.Entries, 1)
	assert.Len(t, redisLogger.Entries, 2)

	entities := make([]*requestCacheEntity, 0)
	missing := engine.LoadByIDs([]uint64{2, 3}, &entities)
	assert.Len(t, missing, 0)
	assert.Equal(t, "b", entities[0].Name)
	assert.Equal(t, "c", entities[1].Name)
	assert.Len(t, DBLogger.Entries, 2)
	assert.Len(t, redisLogger.Entries, 4)
	missing = engine.LoadByIDs([]uint64{2, 3}, &entities)
	assert.Len(t, missing, 0)
	assert.Equal(t, "b", entities[0].Name)
	assert.Equal(t, "c", entities[1].Name)
	assert.Len(t, DBLogger.Entries, 2)
	assert.Len(t, redisLogger.Entries, 4)

	engine.TrackAndFlush(&requestCacheEntity{Name: "f"})
	DBLogger.Entries = make([]*apexLog.Entry, 0)
	redisLogger.Entries = make([]*apexLog.Entry, 0)
	found = engine.LoadByID(6, entity)
	assert.True(t, found)
	assert.Equal(t, uint(6), entity.ID)
	assert.Equal(t, "f", entity.Name)
	assert.Len(t, DBLogger.Entries, 0)
	assert.Len(t, redisLogger.Entries, 0)
	// TODO update row

	totalRows := engine.CachedSearch(&entities, "IndexName", nil, "d")
	assert.Equal(t, totalRows, 2)
	DBLogger.Entries = make([]*apexLog.Entry, 0)
	redisLogger.Entries = make([]*apexLog.Entry, 0)
	totalRows = engine.CachedSearch(&entities, "IndexName", nil, "d")
	assert.Equal(t, totalRows, 2)
	assert.Equal(t, "d", entities[0].Name)
	assert.Equal(t, "d", entities[1].Name)
	assert.Len(t, redisLogger.Entries, 0)
	// TODO update and see if getting new data
}
