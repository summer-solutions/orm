package tests

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

type TestEntityByIDsRedisCache struct {
	orm.ORM `orm:"redisCache"`
	ID      uint
	Name    string
}

type TestEntityByIDsRedisCacheRef struct {
	orm.ORM `orm:"redisCache"`
	ID      uint
	Name    string
}

func TestEntityByIDsRedis(t *testing.T) {
	var entity TestEntityByIDsRedisCache
	var entityRef TestEntityByIDsRedisCacheRef
	engine := PrepareTables(t, &orm.Registry{}, entityRef, entity)

	flusher := orm.Flusher{}
	for i := 1; i <= 10; i++ {
		e := TestEntityByIDsRedisCache{Name: "Name " + strconv.Itoa(i)}
		engine.Init(&e)
		flusher.RegisterEntity(&e)
		e2 := TestEntityByIDsRedisCacheRef{Name: "Name " + strconv.Itoa(i)}
		engine.Init(&e2)
		flusher.RegisterEntity(&e2)
	}
	response, err := flusher.Flush(engine)
	assert.Nil(t, err)
	assert.Len(t, response, 20)

	DBLogger := &TestDatabaseLogger{}
	pool, has := engine.GetMysql()
	assert.True(t, has)
	pool.RegisterLogger(DBLogger)
	CacheLogger := &TestCacheLogger{}
	cache, has := engine.GetRedis()
	assert.True(t, has)
	cache.RegisterLogger(CacheLogger)

	var found []*TestEntityByIDsRedisCache
	missing, err := engine.TryByIDs([]uint64{2, 13, 1}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 2)
	assert.Len(t, missing, 1)
	assert.Equal(t, []uint64{13}, missing)
	assert.Equal(t, uint(2), found[0].ID)
	assert.Equal(t, "Name 2", found[0].Name)
	assert.Equal(t, uint(1), found[1].ID)
	assert.Equal(t, "Name 1", found[1].Name)
	assert.Len(t, DBLogger.Queries, 1)

	missing, err = engine.TryByIDs([]uint64{2, 13, 1}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 2)
	assert.Len(t, missing, 1)
	assert.Equal(t, []uint64{13}, missing)
	assert.Equal(t, uint(2), found[0].ID)
	assert.Equal(t, uint(1), found[1].ID)
	assert.Len(t, DBLogger.Queries, 1)

	missing, err = engine.TryByIDs([]uint64{25, 26, 27}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 0)
	assert.Len(t, missing, 3)
	assert.Len(t, DBLogger.Queries, 2)

	err = cache.FlushDB()
	assert.Nil(t, err)
	DBLogger.Queries = make([]string, 0)
	CacheLogger.Requests = make([]string, 0)

	DBLogger.Queries = make([]string, 0)
	missing, err = engine.TryByIDs([]uint64{8, 9, 10}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 3)
	assert.Len(t, missing, 0)
	assert.Len(t, DBLogger.Queries, 1)

	missing, err = engine.TryByIDs([]uint64{8, 9, 10}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 3)
	assert.Len(t, missing, 0)
	assert.Len(t, DBLogger.Queries, 1)
}

func BenchmarkGetByIDsRedis(b *testing.B) {
	var entity TestEntityByIDsRedisCache
	engine := PrepareTables(&testing.T{}, &orm.Registry{}, entity)

	_ = engine.Flush(&TestEntityByIDsRedisCache{Name: "Hi 1"}, &TestEntityByIDsRedisCache{Name: "Hi 2"}, &TestEntityByIDsRedisCache{Name: "Hi 3"})
	var found []TestEntityByIDsRedisCache
	for n := 0; n < b.N; n++ {
		_, _ = engine.TryByIDs([]uint64{1, 2, 3}, &found)
	}
}
