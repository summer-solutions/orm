package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

type TestEntityFlushLazyReference struct {
	orm.ORM
	ID           uint
	Name         string
	ReferenceOne *TestEntityFlushLazyReference
}

func TestFlushLazyReference(t *testing.T) {
	var entity TestEntityFlushLazyReference
	engine := PrepareTables(t, &orm.Registry{}, entity)

	entity1 := &TestEntityFlushLazyReference{Name: "Name 1"}
	engine.RegisterNewEntity(entity1)
	entity2 := &TestEntityFlushLazyReference{Name: "Name 2"}
	engine.RegisterNewEntity(entity2)
	entity3 := &TestEntityFlushLazyReference{Name: "Name 3"}
	engine.RegisterNewEntity(entity3)
	entity4 := &TestEntityFlushLazyReference{Name: "Name 4"}
	engine.RegisterNewEntity(entity4)

	entity1.ReferenceOne = entity2
	err := entity1.Flush()
	assert.Nil(t, err)
	err = entity2.Flush()
	assert.Nil(t, err)
	err = entity3.Flush()
	assert.Nil(t, err)
	err = entity4.Flush()
	assert.Nil(t, err)

	assert.Equal(t, uint(2), entity1.ReferenceOne.ID)

	has, err := engine.LoadByID(1, &entity)
	assert.True(t, has)
	assert.Nil(t, err)
	assert.Equal(t, uint(2), entity1.ReferenceOne.ID)
}
