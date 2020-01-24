package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"testing"
)

type TestEntityByIdsLocal struct {
	Orm  *orm.ORM `orm:"localCache"`
	Id   uint
	Name string
}

func TestGetByIdsLocal(t *testing.T) {

	var entity TestEntityByIdsLocal
	PrepareTables(entity)

	err := orm.Flush(&TestEntityByIdsLocal{Name: "Hi"}, &TestEntityByIdsLocal{Name: "Hello"})
	assert.Nil(t, err)

	DBLogger := &TestDatabaseLogger{}
	orm.GetMysql().RegisterLogger(DBLogger.Logger())

	var found []TestEntityByIdsLocal
	missing, err := orm.TryByIds([]uint64{2, 3, 1}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 2)
	assert.Len(t, missing, 1)
	assert.Equal(t, []uint64{3}, missing)
	entity = found[0]
	assert.Equal(t, uint(2), entity.Id)
	assert.Equal(t, "Hello", entity.Name)
	entity = found[1]
	assert.Equal(t, uint(1), entity.Id)
	assert.Equal(t, "Hi", entity.Name)
	assert.Len(t, DBLogger.Queries, 1)

	missing, err = orm.TryByIds([]uint64{2, 3, 1}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 2)
	assert.Len(t, missing, 1)
	assert.Equal(t, []uint64{3}, missing)
	entity = found[0]
	assert.Equal(t, uint(2), entity.Id)
	entity = found[1]
	assert.Equal(t, uint(1), entity.Id)
	assert.Len(t, DBLogger.Queries, 1)

	missing, err = orm.TryByIds([]uint64{5, 6, 7}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 0)
	assert.Len(t, missing, 3)
	assert.Len(t, DBLogger.Queries, 2)

}

func BenchmarkGetByIdsLocal(b *testing.B) {
	var entity TestEntityByIdsLocal
	PrepareTables(entity)

	_ = orm.Flush(&TestEntityByIdsLocal{Name: "Hi 1"}, &TestEntityByIdsLocal{Name: "Hi 2"}, &TestEntityByIdsLocal{Name: "Hi 3"})

	var found []TestEntityByIdsLocal
	for n := 0; n < b.N; n++ {
		_, _ = orm.TryByIds([]uint64{1, 2, 3}, found)
	}
}
