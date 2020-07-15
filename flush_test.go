package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type flushEntity struct {
	ORM            `orm:"localCache;redisCache"`
	ID             uint
	City           string `orm:"unique=city"`
	Name           string `orm:"unique=name"`
	NameRequired   string `orm:"required"`
	NameTranslated map[string]string
	Age            int
	Uint           uint
	Year           uint16 `orm:"year"`
	ReferenceOne   *flushEntityReference
	ReferenceTwo   *flushEntityReference
}

type flushEntityReference struct {
	ORM
	ID   uint
	Name string
	Age  int
}

type flushEntityReferenceCascade struct {
	ORM
	ID           uint
	ReferenceOne *flushEntity
	ReferenceTwo *flushEntity `orm:"cascade"`
}

func TestFlush(t *testing.T) {
	var entity *flushEntity
	var reference *flushEntityReference
	var referenceCascade *flushEntityReferenceCascade
	engine := PrepareTables(t, &Registry{}, entity, reference, referenceCascade)

	entity = &flushEntity{Name: "Tom", Age: 12, Uint: 7, Year: 1982, NameRequired: "required"}
	entity.NameTranslated = map[string]string{"pl": "kot", "en": "cat"}
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
	assert.Equal(t, uint(7), entity.Uint)
	assert.Equal(t, uint16(1982), entity.Year)
	assert.Equal(t, "required", entity.NameRequired)
	assert.Equal(t, map[string]string{"pl": "kot", "en": "cat"}, entity.NameTranslated)
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
	entity2.ReferenceTwo = reference
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

	engine.MarkToDelete(entity2)
	assert.True(t, engine.IsDirty(entity2))
	engine.TrackAndFlush(entity2)
	found = engine.LoadByID(10, entity2)
	assert.False(t, found)

	referenceCascade = &flushEntityReferenceCascade{ReferenceOne: entity}
	engine.TrackAndFlush(referenceCascade)
	engine.MarkToDelete(entity)
	assert.True(t, engine.IsDirty(entity))
	assert.PanicsWithError(t, "foreign key error in key `test:flushEntityReferenceCascade:ReferenceOne`", func() {
		engine.TrackAndFlush(entity)
	})
	engine.ClearTrackedEntities()
	referenceCascade.ReferenceOne = nil
	referenceCascade.ReferenceTwo = entity
	engine.TrackAndFlush(referenceCascade)
	engine.LoadByID(1, referenceCascade)
	assert.Nil(t, referenceCascade.ReferenceOne)
	assert.NotNil(t, referenceCascade.ReferenceTwo)
	assert.Equal(t, uint(1), referenceCascade.ReferenceTwo.ID)
	engine.MarkToDelete(entity)
	engine.TrackAndFlush(referenceCascade)
	found = engine.LoadByID(1, referenceCascade)
	assert.False(t, found)
}
