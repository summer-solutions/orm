package orm

import (
	"strconv"
	"testing"

	"github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

type testEntityIndexTestRedis struct {
	ORM       `orm:"redisCache"`
	ID        uint
	Name      string `orm:"length=100;index=FirstIndex"`
	Age       uint16
	IndexAge  *CachedQuery `query:":Age = ? ORDER BY :ID"`
	IndexName *CachedQuery `queryOne:":Name = ?"`
	IndexAll  *CachedQuery `query:""`
}

func TestCachedSearchRedis(t *testing.T) {
	var entity *testEntityIndexTestRedis
	engine := PrepareTables(t, &Registry{}, entity)

	var entities = make([]interface{}, 10)
	for i := 1; i <= 5; i++ {
		e := &testEntityIndexTestRedis{Name: "Name " + strconv.Itoa(i), Age: uint16(10)}
		entities[i-1] = e
		engine.Track(e)
	}
	err := engine.Flush()
	assert.Nil(t, err)
	for i := 6; i <= 10; i++ {
		e := &testEntityIndexTestRedis{Name: "Name " + strconv.Itoa(i), Age: uint16(18)}
		entities[i-1] = e
		engine.Track(e)
	}
	err = engine.Flush()
	assert.Nil(t, err)

	DBLogger := memory.New()
	pool := engine.GetMysql()
	pool.AddLogger(DBLogger)
	pool.SetLogLevel(log.InfoLevel)
	RedisLogger := memory.New()
	cache := engine.GetRedis()
	cache.AddLogger(RedisLogger)
	cache.SetLogLevel(log.InfoLevel)

	pager := &Pager{CurrentPage: 1, PageSize: 100}
	var rows []*testEntityIndexTestRedis
	totalRows, err := engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Nil(t, err)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint(6), rows[0].ID)
	assert.Equal(t, uint(7), rows[1].ID)
	assert.Equal(t, uint(8), rows[2].ID)
	assert.Equal(t, uint(9), rows[3].ID)
	assert.Equal(t, uint(10), rows[4].ID)
	assert.Len(t, DBLogger.Entries, 2)

	pager = &Pager{CurrentPage: 1, PageSize: 100}
	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Nil(t, err)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint(6), rows[0].ID)
	assert.Equal(t, uint(7), rows[1].ID)
	assert.Equal(t, uint(8), rows[2].ID)
	assert.Equal(t, uint(9), rows[3].ID)
	assert.Equal(t, uint(10), rows[4].ID)
	assert.Len(t, DBLogger.Entries, 2)

	pager = &Pager{CurrentPage: 2, PageSize: 4}
	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Nil(t, err)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 1)
	assert.Equal(t, uint(10), rows[0].ID)
	assert.Len(t, DBLogger.Entries, 2)

	pager = &Pager{CurrentPage: 1, PageSize: 5}
	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Nil(t, err)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Len(t, DBLogger.Entries, 4)

	engine.Track(rows[0])
	rows[0].Age = 18
	err = engine.Flush()
	assert.Nil(t, err)

	pager = &Pager{CurrentPage: 1, PageSize: 10}
	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 18)

	assert.Nil(t, err)
	assert.Equal(t, 6, totalRows)
	assert.Len(t, rows, 6)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Equal(t, uint(6), rows[1].ID)
	assert.Len(t, DBLogger.Entries, 7)

	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Nil(t, err)
	assert.Equal(t, 4, totalRows)
	assert.Len(t, rows, 4)
	assert.Equal(t, uint(2), rows[0].ID)
	assert.Len(t, DBLogger.Entries, 8)

	totalRows, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Nil(t, err)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)
	assert.Len(t, DBLogger.Entries, 9)

	engine.MarkToDelete(rows[1])
	err = engine.Flush()
	assert.Nil(t, err)

	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Nil(t, err)
	assert.Equal(t, 3, totalRows)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint(3), rows[0].ID)
	assert.Len(t, DBLogger.Entries, 11)

	totalRows, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Nil(t, err)
	assert.Equal(t, 9, totalRows)
	assert.Len(t, rows, 9)
	assert.Len(t, DBLogger.Entries, 12)

	entity = &testEntityIndexTestRedis{Name: "Name 11", Age: uint16(18)}
	engine.Track(entity)
	err = engine.Flush()
	assert.Nil(t, err)

	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Nil(t, err)
	assert.Equal(t, 7, totalRows)
	assert.Len(t, rows, 7)
	assert.Equal(t, uint(11), rows[6].ID)
	assert.Len(t, DBLogger.Entries, 15)

	totalRows, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Nil(t, err)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)
	assert.Len(t, DBLogger.Entries, 16)

	totalRows, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Nil(t, err)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)
	assert.Len(t, DBLogger.Entries, 16)

	RedisLogger.Entries = make([]*log.Entry, 0)
	_, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Entries, 16)
	assert.Len(t, RedisLogger.Entries, 2)

	entity = &testEntityIndexTestRedis{Name: "Name 12", Age: uint16(18)}
	engine.Track(entity)
	err = engine.Flush()
	assert.Nil(t, err)

	pager = &Pager{CurrentPage: 1, PageSize: 100}
	RedisLogger.Entries = make([]*log.Entry, 0)
	totalRows, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Nil(t, err)
	assert.Equal(t, 11, totalRows)
	assert.Len(t, rows, 11)
	assert.Len(t, DBLogger.Entries, 19)

	RedisLogger.Entries = make([]*log.Entry, 0)
	var entityOne testEntityIndexTestRedis
	has, err := engine.CachedSearchOne(&entityOne, "IndexName", "Name 10")
	assert.Nil(t, err)
	assert.True(t, has)
	assert.Equal(t, uint(10), entityOne.ID)
	assert.Len(t, DBLogger.Entries, 20)
	assert.Len(t, RedisLogger.Entries, 3)

	has, err = engine.CachedSearchOne(&entityOne, "IndexName", "Name 10")
	assert.Nil(t, err)
	assert.True(t, has)
	assert.Equal(t, uint(10), entityOne.ID)
	assert.Len(t, DBLogger.Entries, 20)
	assert.Len(t, RedisLogger.Entries, 5)

	engine.Track(&entityOne)
	entityOne.Name = "Name 10a"
	err = engine.Flush()
	assert.Nil(t, err)
	has, err = engine.CachedSearchOne(&entityOne, "IndexName", "Name 10")
	assert.Nil(t, err)
	assert.False(t, has)
}

func BenchmarkCachedRedis(b *testing.B) {
	var entity testEntityIndexTestRedis
	engine := PrepareTables(&testing.T{}, &Registry{}, entity)

	var entities = make([]interface{}, 10)
	for i := 1; i <= 10; i++ {
		e := testEntityIndexTestRedis{Name: "Name " + strconv.Itoa(i), Age: uint16(18)}
		entities[i-1] = &e
		engine.Track(&e)
	}
	err := engine.Flush()
	assert.Nil(b, err)
	pager := &Pager{CurrentPage: 1, PageSize: 100}
	var rows []testEntityIndexTestRedis
	for n := 0; n < b.N; n++ {
		_, _ = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	}
}
