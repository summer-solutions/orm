package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"testing"
)

type TestEntityInterfaces struct {
	Orm          *orm.ORM
	Id           uint
	Uint         uint
	Name         string
	ReferenceOne *orm.ReferenceOne `orm:"ref=tests.TestEntityInterfacesRef"`
}

type TestEntityInterfacesRef struct {
	Orm *orm.ORM
	Id  uint
}

func (e *TestEntityInterfaces) SetDefaults() {
	e.Uint = 3
	e.Name = "hello"
	e.ReferenceOne.Id = 5
}

func TestInterfaces(t *testing.T) {
	PrepareTables(TestEntityInterfaces{}, TestEntityInterfacesRef{})
	entity := &TestEntityInterfaces{}
	orm.Init(entity)
	assert.Equal(t, uint(3), entity.Uint)
	assert.Equal(t, "hello", entity.Name)
	assert.Equal(t, uint64(5), entity.ReferenceOne.Id)
}
