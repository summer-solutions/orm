package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

type TestEntityInterfaces struct {
	orm.ORM
	ID           uint
	Uint         uint
	Name         string
	ReferenceOne *TestEntityInterfacesRef
	Calculated   int `orm:"ignore"`
}

type TestEntityInterfacesRef struct {
	orm.ORM
	ID uint
}

func (e *TestEntityInterfaces) SetDefaults() {
	e.Uint = 3
	e.Name = "hello"
	e.ReferenceOne = &TestEntityInterfacesRef{ID: 1}
}

func (e *TestEntityInterfaces) AfterSaved(_ *orm.Engine) error {
	e.Calculated = int(e.Uint) + int(e.ReferenceOne.ID)
	return nil
}

func TestInterfaces(t *testing.T) {
	engine := PrepareTables(t, &orm.Registry{}, TestEntityInterfaces{}, TestEntityInterfacesRef{})

	e := &TestEntityInterfacesRef{}
	engine.RegisterEntity(e)
	err := e.Flush()
	assert.Nil(t, err)

	entity := &TestEntityInterfaces{}
	engine.RegisterEntity(entity)
	assert.Equal(t, uint(3), entity.Uint)
	assert.Equal(t, "hello", entity.Name)
	assert.Equal(t, uint(1), entity.ReferenceOne.ID)

	entity.Uint = 5
	err = entity.Flush()
	assert.Nil(t, err)
	assert.Equal(t, 6, entity.Calculated)

	entity.Uint = 10
	err = entity.Flush()
	assert.Nil(t, err)
	assert.Equal(t, 11, entity.Calculated)
}
