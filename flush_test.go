package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type flushEntity struct {
	ORM          `orm:"localCache;redisCache"`
	ID           uint
	City         string `orm:"unique=city"`
	Name         string `orm:"unique=name"`
	Age          int
	ReferenceOne *flushEntityReference
}

type flushEntityReference struct {
	ORM
	ID   uint
	Name string
	Age  int
}

func TestFlush(t *testing.T) {
	var entity *flushEntity
	var reference *flushEntityReference
	engine := PrepareTables(t, &Registry{}, entity, reference)

	entity = &flushEntity{Name: "Tom", Age: 12}
	entity.ReferenceOne = &flushEntityReference{Name: "John", Age: 30}
	assert.True(t, engine.IsDirty(entity))
	assert.True(t, engine.IsDirty(entity.ReferenceOne))
	engine.TrackAndFlush(entity)
	engine.TrackAndFlush(entity)
	assert.True(t, engine.Loaded(entity))
	assert.True(t, engine.Loaded(entity.ReferenceOne))
	assert.False(t, engine.IsDirty(entity))
	assert.False(t, engine.IsDirty(entity.ReferenceOne))
	assert.Equal(t, uint(1), entity.ID)
	assert.Equal(t, uint(1), entity.ReferenceOne.ID)
	assert.True(t, engine.Loaded(entity))
	assert.True(t, engine.Loaded(entity.ReferenceOne))

	found := engine.LoadByID(1, entity)
	assert.True(t, found)
	assert.Equal(t, "Tom", entity.Name)
	assert.Equal(t, 12, entity.Age)
	assert.False(t, engine.IsDirty(entity))
	assert.True(t, engine.Loaded(entity))
	assert.False(t, engine.Loaded(entity.ReferenceOne))
	assert.Equal(t, uint(1), entity.ReferenceOne.ID)
	entity.ReferenceOne.Name = "John 2"
	assert.PanicsWithError(t, "entity is not loaded and can't be updated: orm.flushEntityReference [1]", func() {
		engine.TrackAndFlush(entity.ReferenceOne)
	})
	engine.ClearTrackedEntities()

	reference = &flushEntityReference{}
	found = engine.LoadByID(1, reference)
	assert.True(t, found)
	assert.Equal(t, "John", reference.Name)
	assert.Equal(t, 30, reference.Age)
	assert.False(t, engine.IsDirty(reference))
	assert.True(t, engine.Loaded(reference))

	entity2 := &flushEntity{Name: "Tom", Age: 12}
	assert.PanicsWithError(t, "Duplicate entry 'Tom' for key 'name'", func() {
		engine.TrackAndFlush(entity2)
	})
	entity2.Name = "Lucas"
	entity2.ReferenceOne = &flushEntityReference{ID: 2}
	assert.PanicsWithError(t, "foreign key error in key `test:flushEntity:ReferenceOne`", func() {
		engine.TrackAndFlush(entity2)
	}, "")

	entity2.ReferenceOne = nil
	entity2.Name = "Tom"
	engine.SetOnDuplicateKeyUpdate(NewWhere("Age = ?", 40), entity2)
	engine.TrackAndFlush(entity2)
	assert.Equal(t, uint(1), entity2.ID)
	engine.LoadByID(1, entity)
	assert.Equal(t, "Tom", entity.Name)
	assert.Equal(t, 40, entity.Age)

	entity2 = &flushEntity{Name: "Tom", Age: 12}
	engine.SetOnDuplicateKeyUpdate(NewWhere(""), entity2)
	engine.TrackAndFlush(entity2)
	assert.Equal(t, uint(1), entity2.ID)

	entity2 = &flushEntity{Name: "Arthur", Age: 18}
	engine.SetOnDuplicateKeyUpdate(NewWhere(""), entity2)
	engine.TrackAndFlush(entity2)
	assert.Equal(t, uint(7), entity2.ID)

	entity2 = &flushEntity{Name: "Adam", Age: 20, ID: 10}
	engine.TrackAndFlush(entity2)
	found = engine.LoadByID(10, entity2)
	assert.True(t, found)

	entity2.Age = 21
	assert.True(t, engine.IsDirty(entity2))
	engine.TrackAndFlush(entity2)
	assert.False(t, engine.IsDirty(entity2))
	engine.LoadByID(10, entity2)
	assert.Equal(t, 21, entity2.Age)
}
