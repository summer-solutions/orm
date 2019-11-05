package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"strconv"
	"testing"
)

const TestEntityIndexTestRedisName = "tests.TestEntityIndexTestRedis"

type TestEntityIndexTestRedis struct {
	Orm      orm.ORM `orm:"table=TestCachedSearchRedis;redisCache"`
	Id       uint
	Name     string `orm:"length=100;index=FirstIndex"`
	Age      uint16
	IndexAge orm.CachedQuery `query:":Age = ? ORDER BY :Id"`
	IndexAll orm.CachedQuery `query:""`
}

func TestCachedSearchRedis(t *testing.T) {

	var entity TestEntityIndexTestRedis
	PrepareTables(entity)

	var entities = make([]interface{}, 10)
	for i := 1; i <= 5; i++ {
		e := TestEntityIndexTestRedis{Name: "Name " + strconv.Itoa(i), Age: uint16(10)}
		entities[i-1] = &e
	}
	for i := 6; i <= 10; i++ {
		e := TestEntityIndexTestRedis{Name: "Name " + strconv.Itoa(i), Age: uint16(18)}
		entities[i-1] = &e
	}
	err := orm.Flush(entities...)
	assert.Nil(t, err)

	DBLogger := TestDatabaseLogger{}
	orm.GetMysqlDB("default").AddLogger(&DBLogger)
	RedisLogger := TestCacheLogger{}
	orm.GetRedisCache("default").AddLogger(&RedisLogger)

	pager := orm.NewPager(1, 100)
	rows, totalRows := orm.CachedSearch(TestEntityIndexTestRedisName, "IndexAge", pager, 18)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint(6), rows[0].(TestEntityIndexTestRedis).Id)
	assert.Equal(t, uint(7), rows[1].(TestEntityIndexTestRedis).Id)
	assert.Equal(t, uint(8), rows[2].(TestEntityIndexTestRedis).Id)
	assert.Equal(t, uint(9), rows[3].(TestEntityIndexTestRedis).Id)
	assert.Equal(t, uint(10), rows[4].(TestEntityIndexTestRedis).Id)
	assert.Len(t, DBLogger.Queries, 2)

	pager = orm.NewPager(1, 100)
	rows, totalRows = orm.CachedSearch(TestEntityIndexTestRedisName, "IndexAge", pager, 18)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint(6), rows[0].(TestEntityIndexTestRedis).Id)
	assert.Equal(t, uint(7), rows[1].(TestEntityIndexTestRedis).Id)
	assert.Equal(t, uint(8), rows[2].(TestEntityIndexTestRedis).Id)
	assert.Equal(t, uint(9), rows[3].(TestEntityIndexTestRedis).Id)
	assert.Equal(t, uint(10), rows[4].(TestEntityIndexTestRedis).Id)
	assert.Len(t, DBLogger.Queries, 2)

	pager = orm.NewPager(2, 4)
	rows, totalRows = orm.CachedSearch(TestEntityIndexTestRedisName, "IndexAge", pager, 18)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 1)
	assert.Equal(t, uint(10), rows[0].(TestEntityIndexTestRedis).Id)
	assert.Len(t, DBLogger.Queries, 2)

	pager = orm.NewPager(1, 5)
	rows, totalRows = orm.CachedSearch(TestEntityIndexTestRedisName, "IndexAge", pager, 10)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint(1), rows[0].(TestEntityIndexTestRedis).Id)
	assert.Len(t, DBLogger.Queries, 4)

	entity = rows[0].(TestEntityIndexTestRedis)
	entity.Age = 18
	err = orm.Flush(&entity)
	assert.Nil(t, err)

	pager = orm.NewPager(1, 10)
	rows, totalRows = orm.CachedSearch(TestEntityIndexTestRedisName, "IndexAge", pager, 18)
	assert.Equal(t, 6, totalRows)
	assert.Len(t, rows, 6)
	assert.Equal(t, uint(1), rows[0].(TestEntityIndexTestRedis).Id)
	assert.Equal(t, uint(6), rows[1].(TestEntityIndexTestRedis).Id)
	assert.Len(t, DBLogger.Queries, 7)

	rows, totalRows = orm.CachedSearch(TestEntityIndexTestRedisName, "IndexAge", pager, 10)
	assert.Equal(t, 4, totalRows)
	assert.Len(t, rows, 4)
	assert.Equal(t, uint(2), rows[0].(TestEntityIndexTestRedis).Id)
	assert.Len(t, DBLogger.Queries, 8)

	rows, totalRows = orm.CachedSearch(TestEntityIndexTestRedisName, "IndexAll", pager)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)
	assert.Len(t, DBLogger.Queries, 9)

	entity = rows[1].(TestEntityIndexTestRedis)
	orm.MarkToDelete(entity)
	err = orm.Flush(&entity)
	assert.Nil(t, err)

	rows, totalRows = orm.CachedSearch(TestEntityIndexTestRedisName, "IndexAge", pager, 10)
	assert.Equal(t, 3, totalRows)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint(3), rows[0].(TestEntityIndexTestRedis).Id)
	assert.Len(t, DBLogger.Queries, 11)

	rows, totalRows = orm.CachedSearch(TestEntityIndexTestRedisName, "IndexAll", pager)
	assert.Equal(t, 9, totalRows)
	assert.Len(t, rows, 9)
	assert.Len(t, DBLogger.Queries, 12)

	entity = TestEntityIndexTestRedis{Name: "Name 11", Age: uint16(18)}
	err = orm.Flush(&entity)
	assert.Nil(t, err)

	rows, totalRows = orm.CachedSearch(TestEntityIndexTestRedisName, "IndexAge", pager, 18)
	assert.Equal(t, 7, totalRows)
	assert.Len(t, rows, 7)
	assert.Equal(t, uint(11), rows[6].(TestEntityIndexTestRedis).Id)
	assert.Len(t, DBLogger.Queries, 15)

	rows, totalRows = orm.CachedSearch(TestEntityIndexTestRedisName, "IndexAll", pager)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)
	assert.Len(t, DBLogger.Queries, 16)

	orm.EnableContextCache(100, 1)

	rows, totalRows = orm.CachedSearch(TestEntityIndexTestRedisName, "IndexAll", pager)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)
	assert.Len(t, DBLogger.Queries, 16)

	RedisLogger.Requests = make([]string, 0)
	orm.CachedSearch(TestEntityIndexTestRedisName, "IndexAll", pager)
	assert.Len(t, DBLogger.Queries, 16)
	assert.Len(t, RedisLogger.Requests, 0)

	entity = TestEntityIndexTestRedis{Name: "Name 12", Age: uint16(18)}
	err = orm.Flush(&entity)
	assert.Nil(t, err)

	pager = orm.NewPager(1, 100)
	RedisLogger.Requests = make([]string, 0)
	rows, totalRows = orm.CachedSearch(TestEntityIndexTestRedisName, "IndexAll", pager)
	assert.Equal(t, 11, totalRows)
	assert.Len(t, rows, 11)
	assert.Len(t, DBLogger.Queries, 18)
}

func BenchmarkCachedRedis(b *testing.B) {
	var entity TestEntityIndexTestRedis
	PrepareTables(entity)

	var entities = make([]interface{}, 10)
	for i := 1; i <= 10; i++ {
		e := TestEntityIndexTestRedis{Name: "Name " + strconv.Itoa(i), Age: uint16(18)}
		entities[i-1] = &e
	}
	_ = orm.Flush(entities...)
	pager := orm.NewPager(1, 100)

	for n := 0; n < b.N; n++ {
		orm.CachedSearch(TestEntityIndexTestRedisName, "IndexAge", pager, 18)
	}
}
