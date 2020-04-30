package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testEntityFlushLazyReference struct {
	ORM
	ID           uint
	Name         string
	ReferenceOne *testEntityFlushLazyReference
}

func TestFlushLazyReference(t *testing.T) {
	var entity testEntityFlushLazyReference
	engine := PrepareTables(t, &Registry{}, entity)

	entity1 := &testEntityFlushLazyReference{Name: "Name 1"}
	engine.Track(entity1)
	entity2 := &testEntityFlushLazyReference{Name: "Name 2"}
	engine.Track(entity2)
	entity3 := &testEntityFlushLazyReference{Name: "Name 3"}
	engine.Track(entity3)
	entity4 := &testEntityFlushLazyReference{Name: "Name 4"}
	engine.Track(entity4)

	entity1.ReferenceOne = entity2
	err := engine.Flush()
	assert.Nil(t, err)

	assert.Equal(t, uint(1), entity1.ReferenceOne.ID)

	has, err := engine.LoadByID(2, &entity)
	assert.True(t, has)
	assert.Nil(t, err)
	assert.Equal(t, uint(1), entity1.ReferenceOne.ID)
}
