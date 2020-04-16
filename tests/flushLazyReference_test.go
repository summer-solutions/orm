package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

type TestEntityFlushLazyReference struct {
	Orm          *orm.ORM
	ID           uint
	Name         string
	ReferenceOne *TestEntityFlushLazyReference
}

func TestFlushLazyReference(t *testing.T) {
	var entity TestEntityFlushLazyReference
	engine := PrepareTables(t, &orm.Registry{}, entity)

	entity1 := TestEntityFlushLazyReference{Name: "Name 1"}
	entity2 := TestEntityFlushLazyReference{Name: "Name 2"}
	entity3 := TestEntityFlushLazyReference{Name: "Name 3"}
	entity4 := TestEntityFlushLazyReference{Name: "Name 4"}
	engine.Init(&entity1, &entity2, &entity3, &entity4)

	entity1.ReferenceOne = &entity2

	err := engine.Flush(&entity1, &entity2, &entity3, &entity4)
	assert.Nil(t, err)
	assert.Equal(t, uint(2), entity1.ReferenceOne.ID)

	err = engine.GetByID(1, &entity)
	assert.Nil(t, err)
	assert.Equal(t, uint(2), entity1.ReferenceOne.ID)
}
