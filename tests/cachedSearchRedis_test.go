package tests

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

type TestEntityIndexTestRedis struct {
	orm.ORM   `orm:"redisCache"`
	ID        uint
	Name      string `orm:"length=100;index=FirstIndex"`
	Age       uint16
	IndexAge  *orm.CachedQuery `query:":Age = ? ORDER BY :ID"`
	IndexName *orm.CachedQuery `queryOne:":Name = ?"`
	IndexAll  *orm.CachedQuery `query:""`
}

func TestCachedSearchRedis(t *testing.T) {
	var entity TestEntityIndexTestRedis
	engine := PrepareTables(t, &orm.Registry{}, entity)

	var entities = make([]interface{}, 10)
	for i := 1; i <= 5; i++ {
		e := TestEntityIndexTestRedis{Name: "Name " + strconv.Itoa(i), Age: uint16(10)}
		entities[i-1] = &e
	}
	for i := 6; i <= 10; i++ {
		e := TestEntityIndexTestRedis{Name: "Name " + strconv.Itoa(i), Age: uint16(18)}
		entities[i-1] = &e
	}
	err := engine.Flush(entities...)
	assert.Nil(t, err)

	DBLogger := &TestDatabaseLogger{}
	pool, _ := engine.GetMysql()
	pool.RegisterLogger(DBLogger)
	RedisLogger := &TestCacheLogger{}
	cache, _ := engine.GetRedis()
	cache.RegisterLogger(RedisLogger)

	pager := &orm.Pager{CurrentPage: 1, PageSize: 100}
	var rows []*TestEntityIndexTestRedis
	totalRows, err := engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Nil(t, err)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint(6), rows[0].ID)
	assert.Equal(t, uint(7), rows[1].ID)
	assert.Equal(t, uint(8), rows[2].ID)
	assert.Equal(t, uint(9), rows[3].ID)
	assert.Equal(t, uint(10), rows[4].ID)
	assert.Len(t, DBLogger.Queries, 2)

	pager = &orm.Pager{CurrentPage: 1, PageSize: 100}
	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Nil(t, err)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint(6), rows[0].ID)
	assert.Equal(t, uint(7), rows[1].ID)
	assert.Equal(t, uint(8), rows[2].ID)
	assert.Equal(t, uint(9), rows[3].ID)
	assert.Equal(t, uint(10), rows[4].ID)
	assert.Len(t, DBLogger.Queries, 2)

	pager = &orm.Pager{CurrentPage: 2, PageSize: 4}
	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Nil(t, err)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 1)
	assert.Equal(t, uint(10), rows[0].ID)
	assert.Len(t, DBLogger.Queries, 2)

	pager = &orm.Pager{CurrentPage: 1, PageSize: 5}
	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Nil(t, err)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Len(t, DBLogger.Queries, 4)

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
	assert.Len(t, DBLogger.Queries, 7)

	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Nil(t, err)
	assert.Equal(t, 4, totalRows)
	assert.Len(t, rows, 4)
	assert.Equal(t, uint(2), rows[0].ID)
	assert.Len(t, DBLogger.Queries, 8)

	totalRows, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Nil(t, err)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)
	assert.Len(t, DBLogger.Queries, 9)

	rows[1].MarkToDelete()
	err = engine.Flush(rows[1])
	assert.Nil(t, err)

	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Nil(t, err)
	assert.Equal(t, 3, totalRows)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint(3), rows[0].ID)
	assert.Len(t, DBLogger.Queries, 11)

	totalRows, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Nil(t, err)
	assert.Equal(t, 9, totalRows)
	assert.Len(t, rows, 9)
	assert.Len(t, DBLogger.Queries, 12)

	entity = TestEntityIndexTestRedis{Name: "Name 11", Age: uint16(18)}
	err = engine.Flush(&entity)
	assert.Nil(t, err)

	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Nil(t, err)
	assert.Equal(t, 7, totalRows)
	assert.Len(t, rows, 7)
	assert.Equal(t, uint(11), rows[6].ID)
	assert.Len(t, DBLogger.Queries, 15)

	totalRows, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Nil(t, err)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)
	assert.Len(t, DBLogger.Queries, 16)

	totalRows, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Nil(t, err)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)
	assert.Len(t, DBLogger.Queries, 16)

	RedisLogger.Requests = make([]string, 0)
	_, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Queries, 16)
	assert.Len(t, RedisLogger.Requests, 2)

	entity = TestEntityIndexTestRedis{Name: "Name 12", Age: uint16(18)}
	err = engine.Flush(&entity)
	assert.Nil(t, err)

	pager = &orm.Pager{CurrentPage: 1, PageSize: 100}
	RedisLogger.Requests = make([]string, 0)
	totalRows, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Nil(t, err)
	assert.Equal(t, 11, totalRows)
	assert.Len(t, rows, 11)
	assert.Len(t, DBLogger.Queries, 19)

	RedisLogger.Requests = make([]string, 0)
	var entityOne TestEntityIndexTestRedis
	has, err := engine.CachedSearchOne(&entityOne, "IndexName", "Name 10")
	assert.Nil(t, err)
	assert.True(t, has)
	assert.Equal(t, uint(10), entityOne.ID)
	assert.Len(t, DBLogger.Queries, 20)
	assert.Len(t, RedisLogger.Requests, 3)

	has, err = engine.CachedSearchOne(&entityOne, "IndexName", "Name 10")
	assert.Nil(t, err)
	assert.True(t, has)
	assert.Equal(t, uint(10), entityOne.ID)
	assert.Len(t, DBLogger.Queries, 20)
	assert.Len(t, RedisLogger.Requests, 5)

	entityOne.Name = "Name 10a"
	err = engine.Flush(&entityOne)
	assert.Nil(t, err)
	has, err = engine.CachedSearchOne(&entityOne, "IndexName", "Name 10")
	assert.Nil(t, err)
	assert.False(t, has)
}

func BenchmarkCachedRedis(b *testing.B) {
	var entity TestEntityIndexTestRedis
	engine := PrepareTables(&testing.T{}, &orm.Registry{}, entity)

	var entities = make([]interface{}, 10)
	for i := 1; i <= 10; i++ {
		e := TestEntityIndexTestRedis{Name: "Name " + strconv.Itoa(i), Age: uint16(18)}
		entities[i-1] = &e
	}
	_ = engine.Flush(entities...)
	pager := &orm.Pager{CurrentPage: 1, PageSize: 100}
	var rows []TestEntityIndexTestRedis
	for n := 0; n < b.N; n++ {
		_, _ = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	}
}
