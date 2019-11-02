package tests

import (
	"github.com/stretchr/testify/assert"
	"orm"
	"testing"
)

const TestEntityByIdsRedisCacheName = "tests.TestEntityByIdsRedisCache"

type TestEntityByIdsRedisCache struct {
	Orm  orm.ORM `orm:"table=TestGetByIdsRedis;redisCache"`
	Id   uint
	Name string
}

func TestEntityByIdsRedis(t *testing.T) {

	var entity TestEntityByIdsRedisCache
	PrepareTables(entity)

	err := orm.Flush(&TestEntityByIdsRedisCache{Name: "Hi"}, &TestEntityByIdsRedisCache{Name: "Hello"})
	assert.Nil(t, err)

	DBLogger := TestDatabaseLogger{}
	orm.GetMysqlDB("default").AddLogger(&DBLogger)

	found, missing := orm.TryByIds([]uint64{2, 3, 1}, TestEntityByIdsRedisCacheName)
	assert.Len(t, found, 2)
	assert.Len(t, missing, 1)
	assert.Equal(t, []uint64{3}, missing)
	entity = found[0].(TestEntityByIdsRedisCache)
	assert.Equal(t, uint(2), entity.Id)
	assert.Equal(t, "Hello", entity.Name)
	entity = found[1].(TestEntityByIdsRedisCache)
	assert.Equal(t, uint(1), entity.Id)
	assert.Equal(t, "Hi", entity.Name)
	assert.Len(t, DBLogger.Queries, 1)

	found, missing = orm.TryByIds([]uint64{2, 3, 1}, TestEntityByIdsRedisCacheName)
	assert.Len(t, found, 2)
	assert.Len(t, missing, 1)
	assert.Equal(t, []uint64{3}, missing)
	entity = found[0].(TestEntityByIdsRedisCache)
	assert.Equal(t, uint(2), entity.Id)
	entity = found[1].(TestEntityByIdsRedisCache)
	assert.Equal(t, uint(1), entity.Id)
	assert.Len(t, DBLogger.Queries, 1)

	found, missing = orm.TryByIds([]uint64{5, 6, 7}, TestEntityByIdsRedisCacheName)
	assert.Len(t, found, 0)
	assert.Len(t, missing, 3)
	assert.Len(t, DBLogger.Queries, 2)

}

func BenchmarkGetByIdsRedis(b *testing.B) {
	var entity TestEntityByIdsRedisCache
	PrepareTables(entity)

	_ = orm.Flush(&TestEntityByIdsRedisCache{Name: "Hi 1"}, &TestEntityByIdsRedisCache{Name: "Hi 2"}, &TestEntityByIdsRedisCache{Name: "Hi 3"})

	for n := 0; n < b.N; n++ {
		_, _ = orm.TryByIds([]uint64{1, 2, 3}, TestEntityByIdsRedisCacheName)
	}
}
