package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"testing"
)

type TestEntityFlushLazyReference struct {
	Orm          *orm.ORM
	Id           uint
	Name         string
	ReferenceOne *orm.ReferenceOne `orm:"ref=tests.TestEntityFlushLazyReference"`
}

func TestFlushLazyReference(t *testing.T) {
	var entity TestEntityFlushLazyReference
	engine := PrepareTables(t, &orm.Config{}, entity)

	entity1 := TestEntityFlushLazyReference{Name: "Name 1"}
	entity2 := TestEntityFlushLazyReference{Name: "Name 2"}
	entity3 := TestEntityFlushLazyReference{Name: "Name 3"}
	entity4 := TestEntityFlushLazyReference{Name: "Name 4"}
	engine.Init(&entity1, &entity2, &entity3, &entity4)

	entity1.ReferenceOne.Reference = &entity2

	err := engine.Flush(&entity1, &entity2, &entity3, &entity4)
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), entity1.ReferenceOne.Id)

	err = engine.GetById(1, &entity)
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), entity1.ReferenceOne.Id)
}
