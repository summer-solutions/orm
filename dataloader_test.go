package orm

import (
	"testing"
	"time"

	apexLog "github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

type dataLoaderEntity struct {
	ORM         `orm:"redisCache"`
	ID          uint
	Name        string `orm:"max=100"`
	Description string
}

func TestDataLoader(t *testing.T) {
	var entity *dataLoaderEntity
	engine := PrepareTables(t, &Registry{}, 5, entity)
	engine.EnableDataLoader(100, time.Millisecond)

	engine.Track(&dataLoaderEntity{Name: "a"})
	engine.Track(&dataLoaderEntity{Name: "b"})
	engine.Track(&dataLoaderEntity{Name: "c"})
	engine.Flush()

	DBLogger := memory.New()
	engine.AddQueryLogger(DBLogger, apexLog.InfoLevel, QueryLoggerSourceDB)
	redisLogger := memory.New()
	engine.AddQueryLogger(redisLogger, apexLog.InfoLevel, QueryLoggerSourceRedis)

	entity = &dataLoaderEntity{}
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
	found = engine.LoadByID(2, entity)
	assert.True(t, found)
	assert.Equal(t, uint(2), entity.ID)
	assert.Equal(t, "b", entity.Name)
	assert.Len(t, DBLogger.Entries, 2)
	assert.Len(t, redisLogger.Entries, 4)

	engine.ClearDataLoader()

	go func() {
		entity2 := &dataLoaderEntity{}
		found2 := engine.LoadByID(1, entity2)
		assert.True(t, found2)
		assert.Equal(t, uint(1), entity2.ID)
		assert.Equal(t, "a", entity2.Name)
	}()
	go func() {
		entity3 := &dataLoaderEntity{}
		found3 := engine.LoadByID(2, entity3)
		assert.True(t, found3)
		assert.Equal(t, uint(2), entity3.ID)
		assert.Equal(t, "b", entity3.Name)
	}()
	time.Sleep(time.Millisecond * 10)
	assert.Len(t, DBLogger.Entries, 2)
	assert.Len(t, redisLogger.Entries, 5)

	entities := make([]*dataLoaderEntity, 0)
	missing := engine.LoadByIDs([]uint64{1, 2}, &entities)
	assert.Len(t, missing, 0)
	assert.Len(t, DBLogger.Entries, 2)
	assert.Len(t, redisLogger.Entries, 5)

	missing = engine.LoadByIDs([]uint64{3, 4}, &entities)
	assert.Len(t, missing, 1)
	assert.Equal(t, uint64(4), missing[0])
	assert.Len(t, entities, 1)
	assert.Equal(t, uint(3), entities[0].ID)
	assert.Equal(t, "c", entities[0].Name)
	assert.Len(t, DBLogger.Entries, 3)
	assert.Len(t, redisLogger.Entries, 7)
	missing = engine.LoadByIDs([]uint64{4}, &entities)
	assert.Len(t, missing, 1)
	assert.Equal(t, uint64(4), missing[0])
	assert.Len(t, entities, 0)
	assert.Len(t, DBLogger.Entries, 3)
	assert.Len(t, redisLogger.Entries, 7)

	engine.ClearDataLoader()
	missing = engine.LoadByIDs([]uint64{4, 4}, &entities)
	assert.Len(t, missing, 2)
	assert.Equal(t, uint64(4), missing[0])
	assert.Len(t, DBLogger.Entries, 3)
	assert.Len(t, redisLogger.Entries, 8)

	engine.EnableDataLoader(2, time.Millisecond)
	missing = engine.LoadByIDs([]uint64{3, 1, 2}, &entities)
	assert.Len(t, missing, 0)
	assert.Len(t, entities, 3)
	assert.Equal(t, "c", entities[0].Name)
	assert.Equal(t, "a", entities[1].Name)
	assert.Equal(t, "b", entities[2].Name)
	assert.Len(t, DBLogger.Entries, 3)
	assert.Len(t, redisLogger.Entries, 10)
}