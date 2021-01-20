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
	Ref         *dataLoaderEntityRef
}

type dataLoaderEntityRef struct {
	ORM
	ID   uint
	Name string
}

func TestDataLoader(t *testing.T) {
	var entity *dataLoaderEntity
	var ref *dataLoaderEntityRef
	engine := PrepareTables(t, &Registry{}, 5, entity, ref)

	flusher := engine.NewFlusher()
	flusher.Track(&dataLoaderEntity{Name: "a", Ref: &dataLoaderEntityRef{Name: "r1"}})
	flusher.Track(&dataLoaderEntity{Name: "b", Ref: &dataLoaderEntityRef{Name: "r2"}})
	flusher.Track(&dataLoaderEntity{Name: "c", Ref: &dataLoaderEntityRef{Name: "r3"}})
	flusher.Flush()

	engine.EnableRequestCache(true)

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

	engine.dataLoader.Clear()

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

	engine.dataLoader.Clear()
	missing = engine.LoadByIDs([]uint64{4, 4}, &entities)
	assert.Len(t, missing, 2)
	assert.Equal(t, uint64(4), missing[0])
	assert.Len(t, DBLogger.Entries, 3)
	assert.Len(t, redisLogger.Entries, 8)

	engine.EnableRequestCache(true)
	engine.dataLoader.maxBatchSize = 2
	missing = engine.LoadByIDs([]uint64{3, 1, 2}, &entities)
	assert.Len(t, missing, 0)
	assert.Len(t, entities, 3)
	assert.Equal(t, "c", entities[0].Name)
	assert.Equal(t, "a", entities[1].Name)
	assert.Equal(t, "b", entities[2].Name)
	assert.Len(t, DBLogger.Entries, 3)
	assert.Len(t, redisLogger.Entries, 10)

	engine.dataLoader.Clear()
	DBLogger.Entries = make([]*apexLog.Entry, 0)
	redisLogger.Entries = make([]*apexLog.Entry, 0)
	entity = &dataLoaderEntity{}
	found = engine.LoadByID(1, entity, "Ref")
	assert.True(t, found)
	assert.True(t, entity.Ref.Loaded())
	assert.Equal(t, "r1", entity.Ref.Name)
	assert.Len(t, DBLogger.Entries, 1)
	assert.Len(t, redisLogger.Entries, 1)
	found = engine.LoadByID(1, entity, "Ref")
	assert.True(t, found)
	assert.True(t, entity.Ref.Loaded())
	assert.Equal(t, "r1", entity.Ref.Name)
	assert.Len(t, DBLogger.Entries, 1)
	assert.Len(t, redisLogger.Entries, 1)

	entities[0].Name = "c2"
	engine.Flush(entities[0])
	entity = &dataLoaderEntity{}
	found = engine.LoadByID(3, entity)
	assert.True(t, found)
	assert.Equal(t, "c2", entity.Name)

	engine.GetMysql().Begin()
	entities[0].Name = "c3"
	engine.Flush(entities[0])
	engine.GetMysql().Commit()
	entity = &dataLoaderEntity{}
	engine.LoadByID(3, entity)
	assert.Equal(t, "c3", entity.Name)

	engine.dataLoader.Clear()
	entity = &dataLoaderEntity{Name: "d"}
	engine.Flush(entity)
	DBLogger.Entries = make([]*apexLog.Entry, 0)
	redisLogger.Entries = make([]*apexLog.Entry, 0)
	found = engine.LoadByID(4, entity)
	assert.True(t, found)
	assert.Equal(t, "d", entity.Name)
	assert.Len(t, DBLogger.Entries, 0)
	assert.Len(t, redisLogger.Entries, 0)

	engine.dataLoader.Clear()
	DBLogger.Entries = make([]*apexLog.Entry, 0)
	redisLogger.Entries = make([]*apexLog.Entry, 0)
	entity = &dataLoaderEntity{}
	found = engine.SearchOne(NewWhere("ID = 1"), entity)
	assert.Len(t, DBLogger.Entries, 1)
	assert.Len(t, redisLogger.Entries, 0)
	assert.True(t, found)
	entity = &dataLoaderEntity{}
	found = engine.LoadByID(1, entity)
	assert.True(t, found)
	assert.Equal(t, "a", entity.Name)
	assert.Len(t, DBLogger.Entries, 1)
	assert.Len(t, redisLogger.Entries, 0)

	engine.dataLoader.Clear()
	DBLogger.Entries = make([]*apexLog.Entry, 0)
	redisLogger.Entries = make([]*apexLog.Entry, 0)
	entities = make([]*dataLoaderEntity, 0)
	engine.Search(NewWhere("ID = 1"), nil, &entities)
	assert.Len(t, DBLogger.Entries, 1)
	assert.Len(t, redisLogger.Entries, 0)
	assert.True(t, found)
	entity = &dataLoaderEntity{}
	found = engine.LoadByID(1, entity)
	assert.True(t, found)
	assert.Equal(t, "a", entity.Name)
	assert.Len(t, DBLogger.Entries, 1)
	assert.Len(t, redisLogger.Entries, 0)

	engine.LoadByID(4, entity)
	engine.Delete(entity)
	found = engine.LoadByID(4, entity)
	assert.False(t, found)

	found = engine.LoadByID(20, entity)
	assert.False(t, found)
}
