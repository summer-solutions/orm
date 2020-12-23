package orm

import (
	"testing"

	apexLog "github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

type requestCacheEntity struct {
	ORM
	ID   uint
	Name string `orm:"max=100"`
}

func TestRequestCache(t *testing.T) {
	var entity *requestCacheEntity
	engine := PrepareTables(t, &Registry{}, 5, entity)

	engine.Track(&requestCacheEntity{Name: "a"})
	engine.Track(&requestCacheEntity{Name: "b"})
	engine.Track(&requestCacheEntity{Name: "c"})
	engine.Flush()

	engine.EnableRequestCache(false)

	DBLogger := memory.New()
	engine.AddQueryLogger(DBLogger, apexLog.InfoLevel, QueryLoggerSourceDB)

	entity = &requestCacheEntity{}
	found := engine.LoadByID(1, entity)
	assert.True(t, found)
	assert.Equal(t, uint(1), entity.ID)
	assert.Equal(t, "a", entity.Name)
	assert.Len(t, DBLogger.Entries, 1)

	found = engine.LoadByID(1, entity)
	assert.True(t, found)
	assert.Equal(t, uint(1), entity.ID)
	assert.Equal(t, "a", entity.Name)
	assert.Len(t, DBLogger.Entries, 1)

	entities := make([]*requestCacheEntity, 0)
	missing := engine.LoadByIDs([]uint64{2, 3}, &entities)
	assert.Len(t, missing, 0)
	assert.Len(t, DBLogger.Entries, 2)
	missing = engine.LoadByIDs([]uint64{2, 3}, &entities)
	assert.Len(t, missing, 0)
	assert.Len(t, DBLogger.Entries, 2)
}
