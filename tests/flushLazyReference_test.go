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
	engine.RegisterEntity(entity1)
	entity2 := &TestEntityFlushLazyReference{Name: "Name 2"}
	engine.RegisterEntity(entity2)
	entity3 := &TestEntityFlushLazyReference{Name: "Name 3"}
	engine.RegisterEntity(entity3)
	entity4 := &TestEntityFlushLazyReference{Name: "Name 4"}
	engine.RegisterEntity(entity4)

	entity1.ReferenceOne = entity2
	err := entity1.Flush()
	assert.Nil(t, err)
	err = entity3.Flush()
	assert.Nil(t, err)
	err = entity4.Flush()
	assert.Nil(t, err)

	assert.Equal(t, uint(1), entity1.ReferenceOne.ID)

	has, err := engine.LoadByID(2, &entity)
	assert.True(t, has)
	assert.Nil(t, err)
	assert.Equal(t, uint(1), entity1.ReferenceOne.ID)
}
