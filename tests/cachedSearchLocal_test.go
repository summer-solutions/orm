package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"strconv"
	"testing"
)

const TestEntityIndexTestLocalName = "tests.TestEntityIndexTestLocal"

type TestEntityIndexTestLocal struct {
	Orm      orm.ORM `orm:"table=TestCachedSearchLocal;localCache"`
	Id       uint
	Name     string `orm:"length=100;index=FirstIndex"`
	Age      uint16
	IndexAge orm.CachedQuery `query:":Age = ? ORDER BY :Id"`
	IndexAll orm.CachedQuery `query:""`
}

func TestCachedSearchLocal(t *testing.T) {

	var entity TestEntityIndexTestLocal
	PrepareTables(entity)

	var entities = make([]interface{}, 10)
	for i := 1; i <= 5; i++ {
		e := TestEntityIndexTestLocal{Name: "Name " + strconv.Itoa(i), Age: uint16(10)}
		entities[i-1] = &e
	}
	for i := 6; i <= 10; i++ {
		e := TestEntityIndexTestLocal{Name: "Name " + strconv.Itoa(i), Age: uint16(18)}
		entities[i-1] = &e
	}

	err := orm.Flush(entities...)
	assert.Nil(t, err)

	DBLogger := TestDatabaseLogger{}
	orm.GetMysqlDB("default").AddLogger(&DBLogger)

	pager := orm.NewPager(1, 100)
	rows, totalRows := orm.CachedSearch(TestEntityIndexTestLocalName, "IndexAge", pager, 18)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint(6), rows[0].(TestEntityIndexTestLocal).Id)
	assert.Equal(t, uint(7), rows[1].(TestEntityIndexTestLocal).Id)
	assert.Equal(t, uint(8), rows[2].(TestEntityIndexTestLocal).Id)
	assert.Equal(t, uint(9), rows[3].(TestEntityIndexTestLocal).Id)
	assert.Equal(t, uint(10), rows[4].(TestEntityIndexTestLocal).Id)
	assert.Len(t, DBLogger.Queries, 1)

	rows, totalRows = orm.CachedSearch(TestEntityIndexTestLocalName, "IndexAge", pager, 18)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint(6), rows[0].(TestEntityIndexTestLocal).Id)
	assert.Equal(t, uint(7), rows[1].(TestEntityIndexTestLocal).Id)
	assert.Equal(t, uint(8), rows[2].(TestEntityIndexTestLocal).Id)
	assert.Equal(t, uint(9), rows[3].(TestEntityIndexTestLocal).Id)
	assert.Equal(t, uint(10), rows[4].(TestEntityIndexTestLocal).Id)
	assert.Len(t, DBLogger.Queries, 1)

	pager = orm.NewPager(2, 4)
	rows, totalRows = orm.CachedSearch(TestEntityIndexTestLocalName, "IndexAge", pager, 18)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 1)
	assert.Equal(t, uint(10), rows[0].(TestEntityIndexTestLocal).Id)
	assert.Len(t, DBLogger.Queries, 1)

	pager = orm.NewPager(1, 5)
	rows, totalRows = orm.CachedSearch(TestEntityIndexTestLocalName, "IndexAge", pager, 10)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint(1), rows[0].(TestEntityIndexTestLocal).Id)
	assert.Len(t, DBLogger.Queries, 2)

	entity = rows[0].(TestEntityIndexTestLocal)
	entity.Age = 18
	err = orm.Flush(&entity)
	assert.Nil(t, err)

	pager = orm.NewPager(1, 10)
	rows, totalRows = orm.CachedSearch(TestEntityIndexTestLocalName, "IndexAge", pager, 18)
	assert.Equal(t, 6, totalRows)
	assert.Len(t, rows, 6)
	assert.Equal(t, uint(1), rows[0].(TestEntityIndexTestLocal).Id)
	assert.Equal(t, uint(6), rows[1].(TestEntityIndexTestLocal).Id)
	assert.Len(t, DBLogger.Queries, 4)

	rows, totalRows = orm.CachedSearch(TestEntityIndexTestLocalName, "IndexAge", pager, 10)
	assert.Equal(t, 4, totalRows)
	assert.Len(t, rows, 4)
	assert.Equal(t, uint(2), rows[0].(TestEntityIndexTestLocal).Id)
	assert.Len(t, DBLogger.Queries, 5)

	rows, totalRows = orm.CachedSearch(TestEntityIndexTestLocalName, "IndexAll", pager)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)
	assert.Len(t, DBLogger.Queries, 6)

	entity = rows[1].(TestEntityIndexTestLocal)
	orm.MarkToDelete(entity)
	err = orm.Flush(&entity)
	assert.Nil(t, err)

	rows, totalRows = orm.CachedSearch(TestEntityIndexTestLocalName, "IndexAge", pager, 10)
	assert.Equal(t, 3, totalRows)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint(3), rows[0].(TestEntityIndexTestLocal).Id)
	assert.Len(t, DBLogger.Queries, 8)

	rows, totalRows = orm.CachedSearch(TestEntityIndexTestLocalName, "IndexAll", pager)
	assert.Equal(t, 9, totalRows)
	assert.Len(t, rows, 9)
	assert.Len(t, DBLogger.Queries, 9)

	entity = TestEntityIndexTestLocal{Name: "Name 11", Age: uint16(18)}
	err = orm.Flush(&entity)
	assert.Nil(t, err)

	rows, totalRows = orm.CachedSearch(TestEntityIndexTestLocalName, "IndexAge", pager, 18)
	assert.Equal(t, 7, totalRows)
	assert.Len(t, rows, 7)
	assert.Equal(t, uint(11), rows[6].(TestEntityIndexTestLocal).Id)
	assert.Len(t, DBLogger.Queries, 11)

	rows, totalRows = orm.CachedSearch(TestEntityIndexTestLocalName, "IndexAll", pager)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)
	assert.Len(t, DBLogger.Queries, 12)
}

func BenchmarkCachedSearchLocal(b *testing.B) {
	var entity TestEntityIndexTestLocal
	PrepareTables(entity)

	var entities = make([]interface{}, 10)
	for i := 1; i <= 10; i++ {
		e := TestEntityIndexTestLocal{Name: "Name " + strconv.Itoa(i), Age: uint16(18)}
		entities[i-1] = &e
	}
	_ = orm.Flush(entities...)
	pager := orm.NewPager(1, 100)

	for n := 0; n < b.N; n++ {
		orm.CachedSearch(TestEntityIndexTestLocalName, "IndexAge", pager, 18)
	}
}
