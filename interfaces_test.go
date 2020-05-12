package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testEntityInterfaces struct {
	ORM
	ID           uint
	Uint         uint
	Name         string
	ReferenceOne *testEntityInterfacesRef
	Calculated   int `orm:"ignore"`
}

type testEntityInterfacesRef struct {
	ORM
	ID uint
}

func (e *testEntityInterfaces) SetDefaults() {
	e.Uint = 3
	e.Name = "hello"
	e.ReferenceOne = &testEntityInterfacesRef{ID: 1}
}

func (e *testEntityInterfaces) AfterSaved(_ *Engine) error {
	e.Calculated = int(e.Uint) + int(e.ReferenceOne.ID)
	return nil
}

func TestInterfaces(t *testing.T) {
	engine := PrepareTables(t, &Registry{}, testEntityInterfaces{}, testEntityInterfacesRef{})
	defer engine.Defer()

	e := &testEntityInterfacesRef{}
	err := engine.TrackAndFlush(e)
	assert.Nil(t, err)

	entity := &testEntityInterfaces{}
	engine.Track(entity)
	assert.Equal(t, uint(3), entity.Uint)
	assert.Equal(t, "hello", entity.Name)
	assert.Equal(t, uint(1), entity.ReferenceOne.ID)

	entity.Uint = 5
	err = engine.Flush()
	assert.Nil(t, err)
	assert.Equal(t, 6, entity.Calculated)

	engine.Track(entity)
	entity.Uint = 10
	err = engine.Flush()
	assert.Nil(t, err)
	assert.Equal(t, 11, entity.Calculated)
}
