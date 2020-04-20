package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

type TestEntityByIDsLocal struct {
	orm.ORM `orm:"localCache"`
	ID      uint
	Name    string
}

func TestGetByIDsLocal(t *testing.T) {
	var entity TestEntityByIDsLocal
	engine := PrepareTables(t, &orm.Registry{}, entity)

	e := &TestEntityByIDsLocal{Name: "Hi"}
	engine.RegisterEntity(e)
	err := e.Flush()
	assert.Nil(t, err)
	e = &TestEntityByIDsLocal{Name: "Hello"}
	engine.RegisterEntity(e)
	err = e.Flush()
	assert.Nil(t, err)

	DBLogger := &TestDatabaseLogger{}
	pool, has := engine.GetMysql()
	assert.True(t, has)
	pool.RegisterLogger(DBLogger)

	var found []*TestEntityByIDsLocal
	missing, err := engine.LoadByIDs([]uint64{2, 3, 1}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 2)
	assert.Len(t, missing, 1)
	assert.Equal(t, []uint64{3}, missing)
	assert.Equal(t, uint(2), found[0].ID)
	assert.Equal(t, "Hello", found[0].Name)
	assert.Equal(t, uint(1), found[1].ID)
	assert.Equal(t, "Hi", found[1].Name)
	assert.Len(t, DBLogger.Queries, 1)

	missing, err = engine.LoadByIDs([]uint64{2, 3, 1}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 2)
	assert.Len(t, missing, 1)
	assert.Equal(t, []uint64{3}, missing)
	assert.Equal(t, uint(2), found[0].ID)
	assert.Equal(t, uint(1), found[1].ID)
	assert.Len(t, DBLogger.Queries, 1)

	missing, err = engine.LoadByIDs([]uint64{5, 6, 7}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 0)
	assert.Len(t, missing, 3)
	assert.Len(t, DBLogger.Queries, 2)
}

func BenchmarkGetByIDsLocal(b *testing.B) {
	var entity TestEntityByIDsLocal
	engine := PrepareTables(&testing.T{}, &orm.Registry{}, entity)

	e := &TestEntityByIDsLocal{Name: "Hi 1"}
	err := e.Flush()
	assert.Nil(b, err)
	e = &TestEntityByIDsLocal{Name: "Hi 3"}
	err = e.Flush()
	assert.Nil(b, err)

	var found []*TestEntityByIDsLocal
	for n := 0; n < b.N; n++ {
		_, _ = engine.LoadByIDs([]uint64{1, 2, 3}, &found)
	}
}
