package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type flushEntity struct {
	ORM
	ID           uint
	Name         string
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

	reference = &flushEntityReference{}
	found = engine.LoadByID(1, reference)
	assert.True(t, found)
	assert.Equal(t, "John", reference.Name)
	assert.Equal(t, 30, reference.Age)
	assert.False(t, engine.IsDirty(reference))
	assert.True(t, engine.Loaded(reference))

	engine.TrackAndFlush(entity)
}
