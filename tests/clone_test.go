package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"testing"
)

type TestCloneEntity struct {
	Orm  *orm.ORM `orm:"localCache"`
	Id   uint
	Name string
}

func TestClone(t *testing.T) {
	var entity TestCloneEntity
	PrepareTables(entity)

	entity = TestCloneEntity{Name: "Name 1"}
	err := orm.Init(&entity)
	assert.Nil(t, err)
}
