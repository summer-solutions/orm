package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"testing"
)

type TestEntityFlushLazyReference struct {
	Orm           *orm.ORM
	Id            uint
	Name          string
	ReferenceOne  *orm.ReferenceOne  `orm:"ref=tests.TestEntityFlushLazyReference"`
	ReferenceMany *orm.ReferenceMany `orm:"ref=tests.TestEntityFlushLazyReference"`
}

func TestFlushLazyReference(t *testing.T) {
	var entity TestEntityFlushLazyReference
	PrepareTables(entity)

	entity1 := TestEntityFlushLazyReference{Name: "Name 1"}
	entity2 := TestEntityFlushLazyReference{Name: "Name 2"}
	entity3 := TestEntityFlushLazyReference{Name: "Name 3"}
	entity4 := TestEntityFlushLazyReference{Name: "Name 4"}
	orm.Init(&entity1, &entity2, &entity3, &entity4)

	entity1.ReferenceOne.Reference = &entity2
	entity1.ReferenceMany.AddReference(&entity3, &entity4)

	err := orm.Flush(&entity1, &entity2, &entity3, &entity4)
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), entity1.ReferenceOne.Id)
	assert.Equal(t, []uint64{3, 4}, entity1.ReferenceMany.Ids)

	err = orm.GetById(1, &entity)
	assert.Nil(t, err)
	assert.Equal(t, uint64(2), entity1.ReferenceOne.Id)
	assert.True(t, entity1.ReferenceMany.Has(3))
	assert.True(t, entity1.ReferenceMany.Has(4))
}
