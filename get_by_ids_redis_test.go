package orm

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

type testEntityByIDsRedisCache struct {
	ORM  `orm:"redisCache"`
	ID   uint
	Name string
}

type testEntityByIDsRedisCacheRef struct {
	ORM  `orm:"redisCache"`
	ID   uint
	Name string
}

func TestEntityByIDsRedis(t *testing.T) {
	var entity testEntityByIDsRedisCache
	var entityRef testEntityByIDsRedisCacheRef
	engine := PrepareTables(t, &Registry{}, entityRef, entity)

	for i := 1; i <= 10; i++ {
		e := &testEntityByIDsRedisCache{Name: "Name " + strconv.Itoa(i)}
		engine.Track(e)
		e2 := &testEntityByIDsRedisCacheRef{Name: "Name " + strconv.Itoa(i)}
		engine.Track(e2)
	}
	engine.Flush()

	DBLogger := memory.New()
	engine.AddQueryLogger(DBLogger, log.InfoLevel, QueryLoggerSourceDB)
	CacheLogger := memory.New()
	cache := engine.GetRedis()
	engine.AddQueryLogger(CacheLogger, log.InfoLevel, QueryLoggerSourceRedis)

	var found []*testEntityByIDsRedisCache
	missing := engine.LoadByIDs([]uint64{2, 13, 1}, &found)
	assert.Len(t, found, 2)
	assert.Len(t, missing, 1)
	assert.Equal(t, []uint64{13}, missing)
	assert.Equal(t, uint(2), found[0].ID)
	assert.Equal(t, "Name 2", found[0].Name)
	assert.Equal(t, uint(1), found[1].ID)
	assert.Equal(t, "Name 1", found[1].Name)
	assert.Len(t, DBLogger.Entries, 1)

	missing = engine.LoadByIDs([]uint64{2, 13, 1}, &found)
	assert.Len(t, found, 2)
	assert.Len(t, missing, 1)
	assert.Equal(t, []uint64{13}, missing)
	assert.Equal(t, uint(2), found[0].ID)
	assert.Equal(t, uint(1), found[1].ID)
	assert.Len(t, DBLogger.Entries, 1)

	missing = engine.LoadByIDs([]uint64{25, 26, 27}, &found)
	assert.Len(t, found, 0)
	assert.Len(t, missing, 3)
	assert.Len(t, DBLogger.Entries, 2)

	cache.FlushDB()
	DBLogger.Entries = make([]*log.Entry, 0)
	CacheLogger.Entries = make([]*log.Entry, 0)

	DBLogger.Entries = make([]*log.Entry, 0)
	missing = engine.LoadByIDs([]uint64{8, 9, 10}, &found)
	assert.Len(t, found, 3)
	assert.Len(t, missing, 0)
	assert.Len(t, DBLogger.Entries, 1)

	missing = engine.LoadByIDs([]uint64{8, 9, 10}, &found)
	assert.Len(t, found, 3)
	assert.Len(t, missing, 0)
	assert.Len(t, DBLogger.Entries, 1)
}

func BenchmarkGetByIDsRedis(b *testing.B) {
	var entity testEntityByIDsRedisCache
	engine := PrepareTables(&testing.T{}, &Registry{}, entity)

	for i := 1; i <= 3; i++ {
		e := &testEntityByIDsRedisCache{Name: fmt.Sprintf("Name %d", i)}
		engine.Track(e)
		engine.Flush()
	}

	var found []testEntityByIDsRedisCache
	for n := 0; n < b.N; n++ {
		_ = engine.LoadByIDs([]uint64{1, 2, 3}, &found)
	}
}
