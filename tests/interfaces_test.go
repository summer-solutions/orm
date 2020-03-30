package tests

import (
	"fmt"
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
	Calculated   int               `orm:"ignore"`
}

type TestEntityInterfacesRef struct {
	Orm *orm.ORM
	Id  uint
}

func (e *TestEntityInterfaces) SetDefaults() {
	e.Uint = 3
	e.Name = "hello"
	e.ReferenceOne.Id = 1
}

func (e *TestEntityInterfaces) Validate() error {
	if e.Uint < 5 {
		return fmt.Errorf("uint too low")
	}
	return nil
}

func (e *TestEntityInterfaces) AfterSaved() error {
	e.Calculated = int(e.Uint) + int(e.ReferenceOne.Id)
	return nil
}

func TestInterfaces(t *testing.T) {
	engine := PrepareTables(t, &orm.Config{}, TestEntityInterfaces{}, TestEntityInterfacesRef{})

	err := engine.Flush(&TestEntityInterfacesRef{})
	assert.Nil(t, err)

	entity := &TestEntityInterfaces{}
	engine.Init(entity)
	assert.Equal(t, uint(3), entity.Uint)
	assert.Equal(t, "hello", entity.Name)
	assert.Equal(t, uint64(1), entity.ReferenceOne.Id)

	err = engine.Flush(entity)
	assert.NotNil(t, err)
	assert.Error(t, err, "uint too low")
	entity.Uint = 5
	err = engine.Flush(entity)
	assert.Nil(t, err)
	assert.Equal(t, 6, entity.Calculated)

	entity.Uint = 10
	err = engine.Flush(entity)
	assert.Nil(t, err)
	assert.Equal(t, 11, entity.Calculated)
}
