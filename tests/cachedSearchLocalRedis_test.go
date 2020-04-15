package tests

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

type TestEntityIndexTestLocalRedis struct {
	Orm          *orm.ORM `orm:"localCache;redisCache"`
	ID           uint
	Name         string `orm:"length=100;index=FirstIndex"`
	Age          uint16
	Ignore       uint16            `orm:"ignore"`
	IndexAge     *orm.CachedQuery  `query:":Age = ? ORDER BY :ID"`
	IndexAll     *orm.CachedQuery  `query:""`
	IndexName    *orm.CachedQuery  `queryOne:":Name = ?"`
	ReferenceOne *orm.ReferenceOne `orm:"ref=tests.TestEntityIndexTestLocalRedisRef"`
}

type TestEntityIndexTestLocalRedisRef struct {
	Orm  *orm.ORM
	ID   uint
	Name string
}

func TestCachedSearchLocalRedis(t *testing.T) {
	var entity TestEntityIndexTestLocalRedis
	var entityRef TestEntityIndexTestLocalRedisRef
	engine := PrepareTables(t, &orm.Registry{}, entityRef, entity)

	for i := 1; i <= 5; i++ {
		e := &TestEntityIndexTestLocalRedisRef{Name: "Name " + strconv.Itoa(i)}
		err := engine.Flush(e)
		assert.Nil(t, err)
	}

	var entities = make([]interface{}, 10)
	for i := 1; i <= 5; i++ {
		e := TestEntityIndexTestLocalRedis{Name: "Name " + strconv.Itoa(i), Age: uint16(10)}
		engine.Init(&e)
		e.ReferenceOne.ID = uint64(i)
		entities[i-1] = &e
	}
	for i := 6; i <= 10; i++ {
		e := TestEntityIndexTestLocalRedis{Name: "Name " + strconv.Itoa(i), Age: uint16(18)}
		entities[i-1] = &e
	}

	err := engine.Flush(entities...)
	assert.Nil(t, err)

	pager := &orm.Pager{CurrentPage: 1, PageSize: 100}
	var rows []*TestEntityIndexTestLocalRedis
	totalRows, err := engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Nil(t, err)
	assert.Equal(t, 5, totalRows)

	assert.Len(t, rows, 5)
	assert.Equal(t, uint64(1), rows[0].ReferenceOne.ID)
	assert.Equal(t, uint64(2), rows[1].ReferenceOne.ID)
	assert.Equal(t, uint64(3), rows[2].ReferenceOne.ID)
	assert.Equal(t, uint64(4), rows[3].ReferenceOne.ID)
	assert.Equal(t, uint64(5), rows[4].ReferenceOne.ID)

	DBLogger := &TestDatabaseLogger{}
	pool, _ := engine.GetMysql()
	pool.RegisterLogger(DBLogger)

	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Nil(t, err)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)

	assert.Equal(t, uint(6), rows[0].ID)
	assert.Equal(t, uint(7), rows[1].ID)
	assert.Equal(t, uint(8), rows[2].ID)
	assert.Equal(t, uint(9), rows[3].ID)
	assert.Equal(t, uint(10), rows[4].ID)
	assert.Len(t, DBLogger.Queries, 1)

	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Nil(t, err)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint(6), rows[0].ID)
	assert.Equal(t, uint(7), rows[1].ID)
	assert.Equal(t, uint(8), rows[2].ID)
	assert.Equal(t, uint(9), rows[3].ID)
	assert.Equal(t, uint(10), rows[4].ID)
	assert.Len(t, DBLogger.Queries, 1)

	pager = &orm.Pager{CurrentPage: 2, PageSize: 4}
	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Nil(t, err)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 1)
	assert.Equal(t, uint(10), rows[0].ID)
	assert.Len(t, DBLogger.Queries, 1)

	pager = &orm.Pager{CurrentPage: 1, PageSize: 5}
	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Nil(t, err)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Len(t, DBLogger.Queries, 1)

	rows[0].Age = 18
	err = engine.Flush(rows[0])
	assert.Nil(t, err)

	pager = &orm.Pager{CurrentPage: 1, PageSize: 10}
	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Nil(t, err)
	assert.Equal(t, 6, totalRows)
	assert.Len(t, rows, 6)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Equal(t, uint(6), rows[1].ID)
	assert.Len(t, DBLogger.Queries, 3)

	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Nil(t, err)
	assert.Equal(t, 4, totalRows)
	assert.Len(t, rows, 4)
	assert.Equal(t, uint(2), rows[0].ID)
	assert.Len(t, DBLogger.Queries, 4)

	totalRows, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Nil(t, err)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)
	assert.Len(t, DBLogger.Queries, 5)

	rows[1].Orm.MarkToDelete()
	err = engine.Flush(rows[1])
	assert.Nil(t, err)

	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Nil(t, err)
	assert.Equal(t, 3, totalRows)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint(3), rows[0].ID)
	assert.Len(t, DBLogger.Queries, 7)

	totalRows, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Nil(t, err)
	assert.Equal(t, 9, totalRows)
	assert.Len(t, rows, 9)
	assert.Len(t, DBLogger.Queries, 8)

	entity = TestEntityIndexTestLocalRedis{Name: "Name 11", Age: uint16(18)}
	err = engine.Flush(&entity)
	assert.Nil(t, err)

	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Nil(t, err)
	assert.Equal(t, 7, totalRows)
	assert.Len(t, rows, 7)
	assert.Equal(t, uint(11), rows[6].ID)
	assert.Len(t, DBLogger.Queries, 10)

	totalRows, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Nil(t, err)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)
	assert.Len(t, DBLogger.Queries, 11)

	err = engine.ClearByIDs(&entity, 1, 3)
	assert.Nil(t, err)
	totalRows, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Nil(t, err)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)
	assert.Len(t, DBLogger.Queries, 12)

	var row TestEntityIndexTestLocalRedis
	has, err := engine.CachedSearchOne(&row, "IndexName", "Name 6")
	assert.Nil(t, err)
	assert.True(t, has)
	assert.Equal(t, uint(6), row.ID)

	has, err = engine.CachedSearchOne(&row, "IndexName", "Name 99")
	assert.Nil(t, err)
	assert.False(t, has)

	totalRows, err = engine.CachedSearchWithReferences(&rows, "IndexAll", pager, []interface{}{}, []string{"*"})
	assert.Nil(t, err)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)
	assert.Len(t, DBLogger.Queries, 15)
	assert.NotNil(t, rows[0].ReferenceOne.Reference)
	assert.NotNil(t, rows[2].ReferenceOne.Reference)
}
