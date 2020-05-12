package orm

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

type testEntityIndexTestLocalRedis struct {
	ORM          `orm:"localCache;redisCache"`
	ID           uint
	Name         string `orm:"length=100;index=FirstIndex"`
	Age          uint16
	Ignore       uint16       `orm:"ignore"`
	IndexAge     *CachedQuery `query:":Age = ? ORDER BY :ID"`
	IndexAll     *CachedQuery `query:""`
	IndexName    *CachedQuery `queryOne:":Name = ?"`
	ReferenceOne *testEntityIndexTestLocalRedisRef
}

type testEntityIndexTestLocalRedisRef struct {
	ORM
	ID       uint
	Name     string
	IndexAll *CachedQuery `query:""`
	IndexOne *CachedQuery `queryOne:""`
}

type testEntityIndexTestLocalRedisUnregistered struct {
	ORM
	ID uint
}

func TestCachedSearchLocalRedis(t *testing.T) {
	var entity *testEntityIndexTestLocalRedis
	var entityRef *testEntityIndexTestLocalRedisRef
	engine := PrepareTables(t, &Registry{}, entityRef, entity)
	defer engine.Defer()

	for i := 1; i <= 5; i++ {
		e := &testEntityIndexTestLocalRedisRef{Name: "Name " + strconv.Itoa(i)}
		engine.Track(e)
	}
	err := engine.Flush()
	assert.Nil(t, err)

	var entities = make([]interface{}, 10)
	for i := 1; i <= 5; i++ {
		e := &testEntityIndexTestLocalRedis{Name: "Name " + strconv.Itoa(i), Age: uint16(10)}
		engine.Track(e)
		e.ReferenceOne = &testEntityIndexTestLocalRedisRef{ID: uint(i)}
		entities[i-1] = e
	}
	err = engine.Flush()
	assert.Nil(t, err)
	for i := 6; i <= 10; i++ {
		e := &testEntityIndexTestLocalRedis{Name: "Name " + strconv.Itoa(i), Age: uint16(18)}
		engine.Track(e)
		entities[i-1] = e
	}
	err = engine.Flush()
	assert.Nil(t, err)
	pager := &Pager{CurrentPage: 1, PageSize: 100}
	var rows []*testEntityIndexTestLocalRedis
	totalRows, err := engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Nil(t, err)
	assert.Equal(t, 5, totalRows)
	assert.True(t, engine.Loaded(rows[0]))

	assert.Len(t, rows, 5)
	assert.Equal(t, uint(1), rows[0].ReferenceOne.ID)
	assert.Equal(t, uint(2), rows[1].ReferenceOne.ID)
	assert.Equal(t, uint(3), rows[2].ReferenceOne.ID)
	assert.Equal(t, uint(4), rows[3].ReferenceOne.ID)
	assert.Equal(t, uint(5), rows[4].ReferenceOne.ID)
	assert.Equal(t, "", rows[0].ReferenceOne.Name)
	assert.False(t, engine.Loaded(rows[0].ReferenceOne))

	DBLogger := memory.New()
	pool := engine.GetMysql()
	pool.AddLogger(DBLogger)
	pool.SetLogLevel(log.InfoLevel)

	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Nil(t, err)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)

	assert.Equal(t, uint(6), rows[0].ID)
	assert.Equal(t, uint(7), rows[1].ID)
	assert.Equal(t, uint(8), rows[2].ID)
	assert.Equal(t, uint(9), rows[3].ID)
	assert.Equal(t, uint(10), rows[4].ID)
	assert.Len(t, DBLogger.Entries, 1)

	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Nil(t, err)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint(6), rows[0].ID)
	assert.Equal(t, uint(7), rows[1].ID)
	assert.Equal(t, uint(8), rows[2].ID)
	assert.Equal(t, uint(9), rows[3].ID)
	assert.Equal(t, uint(10), rows[4].ID)
	assert.Len(t, DBLogger.Entries, 1)

	pager = &Pager{CurrentPage: 2, PageSize: 4}
	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Nil(t, err)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 1)
	assert.Equal(t, uint(10), rows[0].ID)
	assert.Len(t, DBLogger.Entries, 1)

	pager = &Pager{CurrentPage: 1, PageSize: 5}
	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Nil(t, err)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Len(t, DBLogger.Entries, 1)

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
	assert.Len(t, DBLogger.Entries, 3)

	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Nil(t, err)
	assert.Equal(t, 4, totalRows)
	assert.Len(t, rows, 4)
	assert.Equal(t, uint(2), rows[0].ID)
	assert.Len(t, DBLogger.Entries, 4)

	totalRows, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Nil(t, err)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)
	assert.Len(t, DBLogger.Entries, 5)

	engine.MarkToDelete(rows[1])
	err = engine.Flush()
	assert.Nil(t, err)

	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Nil(t, err)
	assert.Equal(t, 3, totalRows)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint(3), rows[0].ID)
	assert.Len(t, DBLogger.Entries, 7)

	totalRows, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Nil(t, err)
	assert.Equal(t, 9, totalRows)
	assert.Len(t, rows, 9)
	assert.Len(t, DBLogger.Entries, 8)

	entity = &testEntityIndexTestLocalRedis{Name: "Name 11", Age: uint16(18)}
	engine.Track(entity)
	err = engine.Flush()
	assert.Nil(t, err)

	totalRows, err = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Nil(t, err)
	assert.Equal(t, 7, totalRows)
	assert.Len(t, rows, 7)
	assert.Equal(t, uint(11), rows[6].ID)
	assert.Len(t, DBLogger.Entries, 10)

	totalRows, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Nil(t, err)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)
	assert.Len(t, DBLogger.Entries, 11)

	err = engine.ClearByIDs(entity, 1, 3)
	assert.Nil(t, err)
	totalRows, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Nil(t, err)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)
	assert.Len(t, DBLogger.Entries, 12)

	var row testEntityIndexTestLocalRedis
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
	assert.Len(t, DBLogger.Entries, 15)
	assert.Equal(t, "Name 1", rows[0].ReferenceOne.Name)
	assert.Equal(t, "Name 3", rows[1].ReferenceOne.Name)
	assert.True(t, engine.Loaded(rows[0].ReferenceOne))
	assert.True(t, engine.Loaded(rows[1].ReferenceOne))

	var rows2 []*testEntityIndexTestLocalRedisUnregistered
	_, err = engine.CachedSearch(&rows2, "IndexAll", pager)
	assert.EqualError(t, err, "entity 'orm.testEntityIndexTestLocalRedisUnregistered' is not registered")

	var row2 testEntityIndexTestLocalRedisUnregistered
	_, err = engine.CachedSearchOne(&row2, "IndexAll")
	assert.EqualError(t, err, "entity 'orm.testEntityIndexTestLocalRedisUnregistered' is not registered")

	_, err = engine.CachedSearch(&rows, "IndexAll2", pager)
	assert.EqualError(t, err, "unknown index IndexAll2")

	_, err = engine.CachedSearchOne(&row, "IndexAll2", pager)
	assert.EqualError(t, err, "unknown index IndexAll2")

	var rows3 []*testEntityIndexTestLocalRedisRef
	_, err = engine.CachedSearch(&rows3, "IndexAll", pager)
	assert.EqualError(t, err, "cache search not allowed for entity without cache: 'orm.testEntityIndexTestLocalRedisRef'")

	row3 := &testEntityIndexTestLocalRedisRef{}
	_, err = engine.CachedSearchOne(row3, "IndexOne")
	assert.EqualError(t, err, "cache search not allowed for entity without cache: 'orm.testEntityIndexTestLocalRedisRef'")

	engine.GetLocalCache().Clear()
	r := engine.GetRedis()
	_ = r.FlushDB()
	mockClient := &mockRedisClient{client: r.client}
	r.client = mockClient
	mockClient.HMGetMock = func(key string, fields ...string) ([]interface{}, error) {
		return nil, fmt.Errorf("redis error")
	}
	_, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.EqualError(t, err, "redis error")

	mockClient.HMGetMock = nil
	_ = r.FlushDB()
	mockClient.HMSetMock = func(key string, fields map[string]interface{}) (bool, error) {
		return false, fmt.Errorf("redis error")
	}
	_, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.EqualError(t, err, "redis error")
}
