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
	err := engine.Flush()
	assert.Nil(t, err)

	DBLogger := memory.New()
	engine.AddLogger(DBLogger, log.InfoLevel, LoggerSourceDB)
	CacheLogger := memory.New()
	cache := engine.GetRedis()
	engine.AddLogger(CacheLogger, log.InfoLevel, LoggerSourceRedis)

	var found []*testEntityByIDsRedisCache
	missing, err := engine.LoadByIDs([]uint64{2, 13, 1}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 2)
	assert.Len(t, missing, 1)
	assert.Equal(t, []uint64{13}, missing)
	assert.Equal(t, uint(2), found[0].ID)
	assert.Equal(t, "Name 2", found[0].Name)
	assert.Equal(t, uint(1), found[1].ID)
	assert.Equal(t, "Name 1", found[1].Name)
	assert.Len(t, DBLogger.Entries, 1)

	missing, err = engine.LoadByIDs([]uint64{2, 13, 1}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 2)
	assert.Len(t, missing, 1)
	assert.Equal(t, []uint64{13}, missing)
	assert.Equal(t, uint(2), found[0].ID)
	assert.Equal(t, uint(1), found[1].ID)
	assert.Len(t, DBLogger.Entries, 1)

	missing, err = engine.LoadByIDs([]uint64{25, 26, 27}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 0)
	assert.Len(t, missing, 3)
	assert.Len(t, DBLogger.Entries, 2)

	err = cache.FlushDB()
	assert.Nil(t, err)
	DBLogger.Entries = make([]*log.Entry, 0)
	CacheLogger.Entries = make([]*log.Entry, 0)

	DBLogger.Entries = make([]*log.Entry, 0)
	missing, err = engine.LoadByIDs([]uint64{8, 9, 10}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 3)
	assert.Len(t, missing, 0)
	assert.Len(t, DBLogger.Entries, 1)

	missing, err = engine.LoadByIDs([]uint64{8, 9, 10}, &found)
	assert.Nil(t, err)
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
		err := engine.Flush()
		assert.Nil(b, err)
	}

	var found []testEntityByIDsRedisCache
	for n := 0; n < b.N; n++ {
		_, _ = engine.LoadByIDs([]uint64{1, 2, 3}, &found)
	}
}
