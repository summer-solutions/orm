package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

type TestEntityByIDsLocal struct {
	Orm  *orm.ORM `orm:"localCache"`
	ID   uint
	Name string
}

func TestGetByIDsLocal(t *testing.T) {
	var entity TestEntityByIDsLocal
	engine := PrepareTables(t, &orm.Registry{}, entity)

	err := engine.Flush(&TestEntityByIDsLocal{Name: "Hi"}, &TestEntityByIDsLocal{Name: "Hello"})
	assert.Nil(t, err)

	DBLogger := &TestDatabaseLogger{}
	pool, has := engine.GetMysql()
	assert.True(t, has)
	pool.RegisterLogger(DBLogger.Logger())

	var found []*TestEntityByIDsLocal
	missing, err := engine.TryByIDs([]uint64{2, 3, 1}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 2)
	assert.Len(t, missing, 1)
	assert.Equal(t, []uint64{3}, missing)
	assert.Equal(t, uint(2), found[0].ID)
	assert.Equal(t, "Hello", found[0].Name)
	assert.Equal(t, uint(1), found[1].ID)
	assert.Equal(t, "Hi", found[1].Name)
	assert.Len(t, DBLogger.Queries, 1)

	missing, err = engine.TryByIDs([]uint64{2, 3, 1}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 2)
	assert.Len(t, missing, 1)
	assert.Equal(t, []uint64{3}, missing)
	assert.Equal(t, uint(2), found[0].ID)
	assert.Equal(t, uint(1), found[1].ID)
	assert.Len(t, DBLogger.Queries, 1)

	missing, err = engine.TryByIDs([]uint64{5, 6, 7}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 0)
	assert.Len(t, missing, 3)
	assert.Len(t, DBLogger.Queries, 2)
}

func BenchmarkGetByIDsLocal(b *testing.B) {
	var entity TestEntityByIDsLocal
	engine := PrepareTables(&testing.T{}, &orm.Registry{}, entity)

	_ = engine.Flush(&TestEntityByIDsLocal{Name: "Hi 1"}, &TestEntityByIDsLocal{Name: "Hi 2"}, &TestEntityByIDsLocal{Name: "Hi 3"})

	var found []*TestEntityByIDsLocal
	for n := 0; n < b.N; n++ {
		_, _ = engine.TryByIDs([]uint64{1, 2, 3}, &found)
	}
}
