package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type loadByIdsEntity struct {
	ORM          `orm:"localCache;redisCache"`
	ID           uint
	Name         string `orm:"max=100"`
	ReferenceOne *loadByIdsReference
}

type loadByIdsReference struct {
	ORM          `orm:"localCache;redisCache"`
	ID           uint
	Name         string
	ReferenceTwo *loadByIdsSubReference
}

type loadByIdsSubReference struct {
	ORM  `orm:"localCache;redisCache"`
	ID   uint
	Name string
}

func TestLoadByIds(t *testing.T) {
	var entity *loadByIdsEntity
	var reference *loadByIdsReference
	var subReference *loadByIdsSubReference
	engine := PrepareTables(t, &Registry{}, 5, entity, reference, subReference)

	engine.FlushMany(&loadByIdsEntity{Name: "a", ReferenceOne: &loadByIdsReference{Name: "r1", ReferenceTwo: &loadByIdsSubReference{Name: "s1"}}},
		&loadByIdsEntity{Name: "b", ReferenceOne: &loadByIdsReference{Name: "r2", ReferenceTwo: &loadByIdsSubReference{Name: "s2"}}},
		&loadByIdsEntity{Name: "c"})

	var rows []*loadByIdsEntity
	missing := engine.LoadByIDs([]uint64{1, 2, 3, 4}, &rows, "*")
	assert.Len(t, missing, 1)
	assert.Equal(t, uint64(4), missing[0])
	assert.Len(t, rows, 3)
	assert.Equal(t, "a", rows[0].Name)
	assert.Equal(t, "r1", rows[0].ReferenceOne.Name)
	assert.Equal(t, "b", rows[1].Name)
	assert.Equal(t, "r2", rows[1].ReferenceOne.Name)
	assert.Equal(t, "c", rows[2].Name)
	missing = engine.LoadByIDs([]uint64{1, 2, 3, 4}, &rows, "*")
	assert.Len(t, missing, 1)

	missing = engine.LoadByIDs([]uint64{1, 2, 3, 4}, &rows, "ReferenceOne/ReferenceTwo")
	assert.Len(t, missing, 1)
	assert.Len(t, rows, 3)
	assert.Equal(t, "a", rows[0].Name)
	assert.Equal(t, "r1", rows[0].ReferenceOne.Name)
	assert.Equal(t, "b", rows[1].Name)
	assert.Equal(t, "r2", rows[1].ReferenceOne.Name)
	assert.Equal(t, "c", rows[2].Name)

	missing = engine.LoadByIDs([]uint64{3}, &rows, "ReferenceOne/ReferenceTwo")
	assert.Len(t, missing, 0)

	assert.PanicsWithError(t, "reference invalid in loadByIdsEntity not valid", func() {
		engine.LoadByIDs([]uint64{1}, &rows, "invalid")
	})

	assert.PanicsWithError(t, "reference tag Name not valid", func() {
		engine.LoadByIDs([]uint64{1}, &rows, "Name")
	})

	engine = PrepareTables(t, &Registry{}, 5)
	assert.PanicsWithError(t, "entity 'orm.loadByIdsEntity' is not registered", func() {
		engine.LoadByIDs([]uint64{1}, &rows)
	})
}
