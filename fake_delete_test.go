package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testEntityFakeDelete struct {
	ORM        `orm:"localCache"`
	ID         uint16
	Name       string
	FakeDelete bool
	Uint       uint
	IndexAll   *CachedQuery `query:""`
	IndexName  *CachedQuery `query:":Name = ?"`
}

func TestFakeDelete(t *testing.T) {
	registry := &Registry{}
	engine := PrepareTables(t, registry, testEntityFakeDelete{})
	defer engine.Defer()

	entity := &testEntityFakeDelete{}
	engine.Track(entity)
	entity.Name = "one"
	err := engine.Flush()
	assert.Nil(t, err)
	entity2 := &testEntityFakeDelete{}
	engine.Track(entity2)
	entity2.Name = "two"
	err = engine.Flush()
	assert.Nil(t, err)

	var rows []*testEntityFakeDelete
	total, err := engine.CachedSearch(&rows, "IndexAll", nil)
	assert.Nil(t, err)
	assert.Equal(t, 2, total)
	total, err = engine.CachedSearch(&rows, "IndexName", nil, "two")
	assert.Nil(t, err)
	assert.Equal(t, 1, total)

	engine.MarkToDelete(entity2)
	assert.True(t, entity2.FakeDelete)
	assert.True(t, engine.IsDirty(entity2))
	err = engine.Flush()
	assert.Nil(t, err)
	assert.False(t, engine.IsDirty(entity2))

	total, err = engine.SearchWithCount(NewWhere("1"), nil, &rows)
	assert.Nil(t, err)
	assert.Equal(t, 1, total)
	assert.Equal(t, "one", rows[0].Name)

	has, err := engine.LoadByID(1, entity)
	assert.True(t, has)
	assert.Nil(t, err)
	assert.False(t, entity.FakeDelete)

	has, err = engine.LoadByID(2, entity2)
	assert.True(t, has)
	assert.Nil(t, err)
	assert.True(t, entity2.FakeDelete)

	total, err = engine.CachedSearch(&rows, "IndexAll", nil)
	assert.Nil(t, err)
	assert.Equal(t, 1, total)
	assert.Equal(t, "one", rows[0].Name)
	total, err = engine.CachedSearch(&rows, "IndexName", nil, "two")
	assert.Nil(t, err)
	assert.Equal(t, 0, total)

	engine.ForceMarkToDelete(entity2)
	err = engine.Flush()
	assert.Nil(t, err)
	has, err = engine.LoadByID(2, entity2)
	assert.Nil(t, err)
	assert.False(t, has)

	engine.MarkToDelete(entity)
	err = engine.Flush()
	assert.Nil(t, err)

	has, err = engine.SearchOne(NewWhere("1"), entity)
	assert.False(t, has)
	assert.Nil(t, err)
}
