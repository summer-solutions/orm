package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"strconv"
	"testing"
)

type TestEntityByIdsRedisCache struct {
	Orm  *orm.ORM `orm:"redisCache"`
	Id   uint
	Name string
}

type TestEntityByIdsRedisCacheRef struct {
	Orm  *orm.ORM `orm:"redisCache"`
	Id   uint
	Name string
}

func TestEntityByIdsRedis(t *testing.T) {

	var entity TestEntityByIdsRedisCache
	var entityRef TestEntityByIdsRedisCacheRef
	engine := PrepareTables(t, &orm.Config{}, entityRef, entity)

	flusher := orm.Flusher{}
	for i := 1; i <= 10; i++ {
		e := TestEntityByIdsRedisCache{Name: "Name " + strconv.Itoa(i)}
		engine.Init(&e)
		flusher.RegisterEntity(&e)
		e2 := TestEntityByIdsRedisCacheRef{Name: "Name " + strconv.Itoa(i)}
		engine.Init(&e2)
		flusher.RegisterEntity(&e2)
	}
	err := flusher.Flush(engine)
	assert.Nil(t, err)

	DBLogger := &TestDatabaseLogger{}
	engine.GetMysql().RegisterLogger(DBLogger.Logger())
	CacheLogger := &TestCacheLogger{}
	engine.GetRedis().RegisterLogger(CacheLogger.Logger())

	var found []*TestEntityByIdsRedisCache
	missing, err := engine.TryByIds([]uint64{2, 13, 1}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 2)
	assert.Len(t, missing, 1)
	assert.Equal(t, []uint64{13}, missing)
	assert.Equal(t, uint(2), found[0].Id)
	assert.Equal(t, "Name 2", found[0].Name)
	assert.Equal(t, uint(1), found[1].Id)
	assert.Equal(t, "Name 1", found[1].Name)
	assert.Len(t, DBLogger.Queries, 1)

	missing, err = engine.TryByIds([]uint64{2, 13, 1}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 2)
	assert.Len(t, missing, 1)
	assert.Equal(t, []uint64{13}, missing)
	assert.Equal(t, uint(2), found[0].Id)
	assert.Equal(t, uint(1), found[1].Id)
	assert.Len(t, DBLogger.Queries, 1)

	missing, err = engine.TryByIds([]uint64{25, 26, 27}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 0)
	assert.Len(t, missing, 3)
	assert.Len(t, DBLogger.Queries, 2)

	err = engine.GetRedis().FlushDB()
	assert.Nil(t, err)
	DBLogger.Queries = make([]string, 0)
	CacheLogger.Requests = make([]string, 0)

	DBLogger.Queries = make([]string, 0)
	missing, err = engine.TryByIds([]uint64{8, 9, 10}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 3)
	assert.Len(t, missing, 0)
	assert.Len(t, DBLogger.Queries, 1)

	missing, err = engine.TryByIds([]uint64{8, 9, 10}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 3)
	assert.Len(t, missing, 0)
	assert.Len(t, DBLogger.Queries, 1)
}

func BenchmarkGetByIdsRedis(b *testing.B) {
	var entity TestEntityByIdsRedisCache
	engine := PrepareTables(&testing.T{}, &orm.Config{}, entity)

	_ = engine.Flush(&TestEntityByIdsRedisCache{Name: "Hi 1"}, &TestEntityByIdsRedisCache{Name: "Hi 2"}, &TestEntityByIdsRedisCache{Name: "Hi 3"})
	var found []TestEntityByIdsRedisCache
	for n := 0; n < b.N; n++ {
		_, _ = engine.TryByIds([]uint64{1, 2, 3}, &found)
	}
}
