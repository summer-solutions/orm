package orm

import (
	"strconv"
	"testing"

	"github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

type testEntityIndexTestLocal struct {
	ORM          `orm:"localCache"`
	ID           uint
	Name         string `orm:"length=100;index=FirstIndex"`
	Age          uint16
	Ignore       uint16       `orm:"ignore"`
	IndexAge     *CachedQuery `query:":Age = ? ORDER BY :ID"`
	IndexAll     *CachedQuery `query:""`
	IndexName    *CachedQuery `queryOne:":Name = ?"`
	ReferenceOne *testEntityIndexTestLocalRef
}

type testEntityIndexTestLocalRef struct {
	ORM
	ID   uint
	Name string
}

func TestCachedSearchLocal(t *testing.T) {
	var entity *testEntityIndexTestLocal
	var entityRef *testEntityIndexTestLocalRef
	engine := PrepareTables(t, &Registry{}, entityRef, entity)

	for i := 1; i <= 5; i++ {
		e := &testEntityIndexTestLocalRef{Name: "Name " + strconv.Itoa(i)}
		engine.Track(e)
	}
	err := engine.Flush()
	assert.Nil(t, err)

	var entities = make([]interface{}, 10)
	for i := 1; i <= 5; i++ {
		e := &testEntityIndexTestLocal{Name: "Name " + strconv.Itoa(i), Age: uint16(10)}
		engine.Track(e)
		e.ReferenceOne = &testEntityIndexTestLocalRef{ID: uint(i)}
		entities[i-1] = e
	}
	err = engine.Flush()
	assert.Nil(t, err)
	for i := 6; i <= 10; i++ {
		e := &testEntityIndexTestLocal{Name: "Name " + strconv.Itoa(i), Age: uint16(18)}
		entities[i-1] = e
		engine.Track(e)
	}
	err = engine.Flush()
	assert.Nil(t, err)

	pager := &Pager{CurrentPage: 1, PageSize: 100}
	var rows []*testEntityIndexTestLocal
	totalRows, err := engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Nil(t, err)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint(1), rows[0].ReferenceOne.ID)
	assert.Equal(t, uint(2), rows[1].ReferenceOne.ID)
	assert.Equal(t, uint(3), rows[2].ReferenceOne.ID)
	assert.Equal(t, uint(4), rows[3].ReferenceOne.ID)
	assert.Equal(t, uint(5), rows[4].ReferenceOne.ID)

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

	entity = &testEntityIndexTestLocal{Name: "Name 11", Age: uint16(18)}
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

	var row testEntityIndexTestLocal
	has, err := engine.CachedSearchOne(&row, "IndexName", "Name 6")
	assert.Nil(t, err)
	assert.True(t, has)
	assert.Equal(t, uint(6), row.ID)

	has, err = engine.CachedSearchOneWithReferences(&row, "IndexName", []interface{}{"Name 4"}, []string{"ReferenceOne"})
	assert.Nil(t, err)
	assert.True(t, has)
	assert.Equal(t, uint(4), row.ID)
	assert.NotNil(t, row.ReferenceOne)
	assert.Equal(t, "Name 4", row.ReferenceOne.Name)

	has, err = engine.CachedSearchOne(&row, "IndexName", "Name 99")
	assert.Nil(t, err)
	assert.False(t, has)

	pager = &Pager{CurrentPage: 49, PageSize: 1000}
	totalRows, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Nil(t, err)
	assert.Equal(t, 10, totalRows)

	pager = &Pager{CurrentPage: 51, PageSize: 1000}
	totalRows, err = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.EqualError(t, err, "max cache index page size (50000) exceeded IndexAll")
	assert.Equal(t, 0, totalRows)
}

func BenchmarkCachedSearchLocal(b *testing.B) {
	var entity testEntityIndexTestLocal
	var entityRef testEntityIndexTestLocalRef
	engine := PrepareTables(&testing.T{}, &Registry{}, entity, entityRef)

	var entities = make([]interface{}, 10)
	for i := 1; i <= 10; i++ {
		e := testEntityIndexTestLocal{Name: "Name " + strconv.Itoa(i), Age: uint16(18)}
		entities[i-1] = &e
		engine.Track(&e)
	}
	err := engine.Flush()
	assert.Nil(b, err)
	pager := &Pager{CurrentPage: 1, PageSize: 100}
	var rows []*testEntityIndexTestLocal
	for n := 0; n < b.N; n++ {
		_, _ = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	}
}
