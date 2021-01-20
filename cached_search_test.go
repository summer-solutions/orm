package orm

import (
	"strconv"
	"testing"
	"time"

	apexLog "github.com/apex/log"
	"github.com/apex/log/handlers/memory"
	"github.com/stretchr/testify/assert"
)

type cachedSearchEntity struct {
	ORM
	ID             uint
	Name           string `orm:"length=100;unique=FirstIndex"`
	Age            uint16 `orm:"index=SecondIndex"`
	Added          *time.Time
	ReferenceOne   *cachedSearchRefEntity `orm:"index=IndexReference"`
	Ignore         uint16                 `orm:"ignore"`
	IndexAge       *CachedQuery           `query:":Age = ? ORDER BY :Age"`
	IndexAll       *CachedQuery           `query:""`
	IndexName      *CachedQuery           `queryOne:":Name = ?"`
	IndexReference *CachedQuery           `query:":ReferenceOne = ?"`
	FakeDelete     bool                   `orm:"unique=FirstIndex:2;index=IndexReference:2,SecondIndex:2"`
}

type cachedSearchRefEntity struct {
	ORM
	ID        uint
	Name      string       `orm:"unique=FirstIndex"`
	IndexName *CachedQuery `queryOne:":Name = ?"`
	IndexAll  *CachedQuery `query:""`
}

func TestCachedSearchLocal(t *testing.T) {
	testCachedSearch(t, true, false)
}

func TestCachedSearchRedis(t *testing.T) {
	testCachedSearch(t, false, true)
}

func TestCachedSearchLocalRedis(t *testing.T) {
	testCachedSearch(t, true, true)
}

func testCachedSearch(t *testing.T, localCache bool, redisCache bool) {
	var entity *cachedSearchEntity
	var entityRef *cachedSearchRefEntity
	engine := PrepareTables(t, &Registry{}, 5, entityRef, entity)
	schema := engine.GetRegistry().GetTableSchemaForEntity(entity).(*tableSchema)
	if localCache {
		schema.localCacheName = "default"
		schema.hasLocalCache = true
	} else {
		schema.localCacheName = ""
		schema.hasLocalCache = false
	}
	if redisCache {
		schema.redisCacheName = "default"
		schema.hasRedisCache = true
	}

	flusher := engine.NewFlusher()
	for i := 1; i <= 5; i++ {
		flusher.Track(&cachedSearchRefEntity{Name: "Name " + strconv.Itoa(i)})
	}
	flusher.Flush()

	var entities = make([]interface{}, 10)
	for i := 1; i <= 5; i++ {
		e := &cachedSearchEntity{Name: "Name " + strconv.Itoa(i), Age: uint16(10)}
		flusher.Track(e)
		e.ReferenceOne = &cachedSearchRefEntity{ID: uint(i)}
		entities[i-1] = e
	}
	flusher.Flush()
	for i := 6; i <= 10; i++ {
		e := &cachedSearchEntity{Name: "Name " + strconv.Itoa(i), Age: uint16(18)}
		entities[i-1] = e
		flusher.Track(e)
	}
	flusher.Flush()

	pager := NewPager(1, 100)
	var rows []*cachedSearchEntity
	totalRows := engine.CachedSearch(&rows, "IndexAge", nil, 10)
	assert.EqualValues(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint(1), rows[0].ReferenceOne.ID)
	assert.Equal(t, uint(2), rows[1].ReferenceOne.ID)
	assert.Equal(t, uint(3), rows[2].ReferenceOne.ID)
	assert.Equal(t, uint(4), rows[3].ReferenceOne.ID)
	assert.Equal(t, uint(5), rows[4].ReferenceOne.ID)

	totalRows = engine.CachedSearchCount(entity, "IndexAge", 10)
	assert.EqualValues(t, 5, totalRows)

	totalRows = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)

	assert.Equal(t, uint(6), rows[0].ID)
	assert.Equal(t, uint(7), rows[1].ID)
	assert.Equal(t, uint(8), rows[2].ID)
	assert.Equal(t, uint(9), rows[3].ID)
	assert.Equal(t, uint(10), rows[4].ID)

	DBLogger := memory.New()
	engine.AddQueryLogger(DBLogger, apexLog.InfoLevel, QueryLoggerSourceDB)
	totalRows = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint(6), rows[0].ID)
	assert.Equal(t, uint(7), rows[1].ID)
	assert.Equal(t, uint(8), rows[2].ID)
	assert.Equal(t, uint(9), rows[3].ID)
	assert.Equal(t, uint(10), rows[4].ID)
	assert.Len(t, DBLogger.Entries, 0)

	pager = NewPager(2, 4)
	totalRows = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 1)
	assert.Equal(t, uint(10), rows[0].ID)
	assert.Len(t, DBLogger.Entries, 0)

	pager = NewPager(1, 5)
	totalRows = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Equal(t, 5, totalRows)
	assert.Len(t, rows, 5)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Len(t, DBLogger.Entries, 0)

	rows[0].Age = 18
	engine.Flush(rows[0])

	pager = NewPager(1, 10)
	totalRows = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Equal(t, 6, totalRows)
	assert.Len(t, rows, 6)
	assert.Equal(t, uint(1), rows[0].ID)
	assert.Equal(t, uint(6), rows[1].ID)

	totalRows = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Equal(t, 4, totalRows)
	assert.Len(t, rows, 4)
	assert.Equal(t, uint(2), rows[0].ID)

	totalRows = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)

	engine.Delete(rows[1])

	totalRows = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Equal(t, 3, totalRows)
	assert.Len(t, rows, 3)
	assert.Equal(t, uint(3), rows[0].ID)

	totalRows = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Equal(t, 9, totalRows)
	assert.Len(t, rows, 9)

	entity = &cachedSearchEntity{Name: "Name 11", Age: uint16(18)}
	engine.Flush(entity)

	totalRows = engine.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Equal(t, 7, totalRows)
	assert.Len(t, rows, 7)
	assert.Equal(t, uint(11), rows[6].ID)

	totalRows = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)

	engine.ClearByIDs(entity, 1, 3)
	totalRows = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Equal(t, 10, totalRows)
	assert.Len(t, rows, 10)

	var row cachedSearchEntity
	has := engine.CachedSearchOne(&row, "IndexName", "Name 6")
	assert.True(t, has)
	assert.Equal(t, uint(6), row.ID)

	DBLogger.Entries = make([]*apexLog.Entry, 0)
	has = engine.CachedSearchOne(&row, "IndexName", "Name 6")
	assert.True(t, has)
	assert.Equal(t, uint(6), row.ID)
	assert.Len(t, DBLogger.Entries, 0)

	has = engine.CachedSearchOneWithReferences(&row, "IndexName", []interface{}{"Name 4"}, []string{"ReferenceOne"})
	assert.True(t, has)
	assert.Equal(t, uint(4), row.ID)
	assert.NotNil(t, row.ReferenceOne)
	assert.Equal(t, "Name 4", row.ReferenceOne.Name)

	has = engine.CachedSearchOne(&row, "IndexName", "Name 99")
	assert.False(t, has)

	pager = NewPager(49, 1000)
	totalRows = engine.CachedSearch(&rows, "IndexAll", pager)
	assert.Equal(t, 10, totalRows)
	totalRows = engine.CachedSearch(&rows, "IndexAge", nil, 10)
	assert.Equal(t, 3, totalRows)

	totalRows, ids := engine.CachedSearchIDs(entity, "IndexAge", nil, 10)
	assert.Equal(t, 3, totalRows)
	assert.Len(t, ids, 3)
	assert.Equal(t, []uint64{3, 4, 5}, ids)

	totalRows = engine.CachedSearchWithReferences(&rows, "IndexAge", nil, []interface{}{10}, []string{"ReferenceOne"})
	assert.Equal(t, 3, totalRows)
	assert.Equal(t, "Name 3", rows[0].ReferenceOne.Name)
	assert.Equal(t, "Name 4", rows[1].ReferenceOne.Name)
	assert.Equal(t, "Name 5", rows[2].ReferenceOne.Name)
}

func TestCachedSearchErrors(t *testing.T) {
	engine := PrepareTables(t, &Registry{}, 5)
	var rows []*cachedSearchEntity
	assert.PanicsWithError(t, "entity 'orm.cachedSearchEntity' is not registered", func() {
		_ = engine.CachedSearch(&rows, "IndexAge", nil, 10)
	})
	var row cachedSearchEntity
	assert.PanicsWithError(t, "entity 'orm.cachedSearchEntity' is not registered", func() {
		_ = engine.CachedSearchOne(&row, "IndexName", 10)
	})

	var entity *cachedSearchEntity
	var entityRef *cachedSearchRefEntity
	engine = PrepareTables(t, &Registry{}, 5, entity, entityRef)
	assert.PanicsWithError(t, "index InvalidIndex not found", func() {
		_ = engine.CachedSearch(&rows, "InvalidIndex", nil, 10)
	})

	assert.PanicsWithError(t, "index InvalidIndex not found", func() {
		_ = engine.CachedSearchOne(&row, "InvalidIndex", 10)
	})

	pager := NewPager(51, 1000)
	assert.PanicsWithError(t, "max cache index page size (50000) exceeded IndexAge", func() {
		_ = engine.CachedSearch(&rows, "IndexAge", pager, 10)
	})

	var rows2 []*cachedSearchRefEntity
	assert.PanicsWithError(t, "cache search not allowed for entity without cache: 'orm.cachedSearchRefEntity'", func() {
		_ = engine.CachedSearch(&rows2, "IndexAll", nil, 10)
	})

	var row2 cachedSearchRefEntity
	assert.PanicsWithError(t, "cache search not allowed for entity without cache: 'orm.cachedSearchRefEntity'", func() {
		_ = engine.CachedSearchOne(&row2, "IndexName", 10)
	})
}
