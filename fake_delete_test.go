package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testEntityFakeDelete struct {
	ORM        `orm:"localCache"`
	ID         uint16
	Name       string `orm:"index=NameIndex"`
	FakeDelete bool   `orm:"index=NameIndex:2"`
	Uint       uint
	IndexAll   *CachedQuery `query:""`
	IndexName  *CachedQuery `query:":Name = ?"`
}

func TestFakeDelete(t *testing.T) {
	registry := &Registry{}
	engine := PrepareTables(t, registry, testEntityFakeDelete{})

	entity := &testEntityFakeDelete{}
	engine.Track(entity)
	entity.Name = "one"
	engine.Flush()
	entity2 := &testEntityFakeDelete{}
	engine.Track(entity2)
	entity2.Name = "two"
	engine.Flush()

	var rows []*testEntityFakeDelete
	total := engine.CachedSearch(&rows, "IndexAll", nil)
	assert.Equal(t, 2, total)
	total = engine.CachedSearch(&rows, "IndexName", nil, "two")
	assert.Equal(t, 1, total)

	engine.MarkToDelete(entity2)
	assert.True(t, entity2.FakeDelete)
	assert.True(t, engine.IsDirty(entity2))
	engine.Flush()
	assert.False(t, engine.IsDirty(entity2))

	total = engine.SearchWithCount(NewWhere("1"), nil, &rows)
	assert.Equal(t, 1, total)
	assert.Equal(t, "one", rows[0].Name)

	has := engine.LoadByID(1, entity)
	assert.True(t, has)
	assert.False(t, entity.FakeDelete)

	has = engine.LoadByID(2, entity2)
	assert.True(t, has)
	assert.True(t, entity2.FakeDelete)

	total = engine.CachedSearch(&rows, "IndexAll", nil)
	assert.Equal(t, 1, total)
	assert.Equal(t, "one", rows[0].Name)
	total = engine.CachedSearch(&rows, "IndexName", nil, "two")
	assert.Equal(t, 0, total)

	engine.ForceMarkToDelete(entity2)
	engine.Flush()
	has = engine.LoadByID(2, entity2)
	assert.False(t, has)

	engine.MarkToDelete(entity)
	engine.Flush()

	has = engine.SearchOne(NewWhere("1"), entity)
	assert.False(t, has)
}
