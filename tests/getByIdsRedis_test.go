package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"strconv"
	"testing"
)

const TestEntityByIdsRedisCacheName = "tests.TestEntityByIdsRedisCache"

type TestEntityByIdsRedisCache struct {
	Orm            orm.ORM `orm:"table=TestGetByIdsRedis;redisCache"`
	Id             uint
	Name           string
	ReferenceOneId uint16 `orm:"ref=tests.TestEntityByIdsRedisCache"`
	ReferenceTwoId uint16 `orm:"ref=tests.TestEntityByIdsRedisCacheRef"`
}

const TestEntityByIdsRedisCacheRefName = "tests.TestEntityByIdsRedisCacheRef"

type TestEntityByIdsRedisCacheRef struct {
	Orm            orm.ORM `orm:"table=TestEntityByIdsRedisCacheRef;redisCache"`
	Id             uint
	Name           string
	ReferenceOneId uint16 `orm:"ref=tests.TestEntityByIdsRedisCache"`
}

func (e TestEntityByIdsRedisCache) GetReferenceOne() TestEntityByIdsRedisCache {
	return orm.GetById(uint64(e.ReferenceOneId), TestEntityByIdsRedisCacheName).(TestEntityByIdsRedisCache)
}

func (e TestEntityByIdsRedisCache) GetReferenceTwo() TestEntityByIdsRedisCacheRef {
	return orm.GetById(uint64(e.ReferenceTwoId), TestEntityByIdsRedisCacheRefName).(TestEntityByIdsRedisCacheRef)
}

func (e TestEntityByIdsRedisCacheRef) GetReferenceOne() TestEntityByIdsRedisCache {
	return orm.GetById(uint64(e.ReferenceOneId), TestEntityByIdsRedisCacheName).(TestEntityByIdsRedisCache)
}

func TestEntityByIdsRedis(t *testing.T) {

	var entity TestEntityByIdsRedisCache
	var entityRef TestEntityByIdsRedisCacheRef
	PrepareTables(entity, entityRef)

	flusher := orm.NewFlusher(100, false)
	for i := 1; i <= 10; i++ {
		var ref1, ref2 uint16
		ref1 = uint16(i - 1)
		if i >= 7 {
			ref1 = uint16(i - 5)
		}
		if i >= 5 {
			ref2 = uint16(i - 3)
		}
		e := TestEntityByIdsRedisCache{Name: "Name " + strconv.Itoa(i), ReferenceOneId: ref1, ReferenceTwoId: ref2}
		flusher.RegisterEntity(&e)
		e2 := TestEntityByIdsRedisCacheRef{Name: "Name " + strconv.Itoa(i), ReferenceOneId: ref1}
		flusher.RegisterEntity(&e2)
	}
	err := flusher.Flush()
	assert.Nil(t, err)

	DBLogger := TestDatabaseLogger{}
	orm.GetMysqlDB("default").AddLogger(&DBLogger)
	CacheLogger := TestCacheLogger{}
	orm.GetRedisCache("default").AddLogger(&CacheLogger)

	found, missing := orm.TryByIds([]uint64{2, 13, 1}, TestEntityByIdsRedisCacheName)
	assert.Len(t, found, 2)
	assert.Len(t, missing, 1)
	assert.Equal(t, []uint64{13}, missing)
	entity = found[0].(TestEntityByIdsRedisCache)
	assert.Equal(t, uint(2), entity.Id)
	assert.Equal(t, "Name 2", entity.Name)
	entity = found[1].(TestEntityByIdsRedisCache)
	assert.Equal(t, uint(1), entity.Id)
	assert.Equal(t, "Name 1", entity.Name)
	assert.Len(t, DBLogger.Queries, 1)

	found, missing = orm.TryByIds([]uint64{2, 13, 1}, TestEntityByIdsRedisCacheName)
	assert.Len(t, found, 2)
	assert.Len(t, missing, 1)
	assert.Equal(t, []uint64{13}, missing)
	entity = found[0].(TestEntityByIdsRedisCache)
	assert.Equal(t, uint(2), entity.Id)
	entity = found[1].(TestEntityByIdsRedisCache)
	assert.Equal(t, uint(1), entity.Id)
	assert.Len(t, DBLogger.Queries, 1)

	found, missing = orm.TryByIds([]uint64{25, 26, 27}, TestEntityByIdsRedisCacheName)
	assert.Len(t, found, 0)
	assert.Len(t, missing, 3)
	assert.Len(t, DBLogger.Queries, 2)

	orm.GetRedisCache("default").FlushDB()
	DBLogger.Queries = make([]string, 0)

	found, missing = orm.TryByIds([]uint64{8, 9, 10}, TestEntityByIdsRedisCacheName, "ReferenceOneId", "ReferenceTwoId/ReferenceOneId")
	assert.Len(t, found, 3)
	assert.Len(t, missing, 0)
	assert.Len(t, DBLogger.Queries, 4)

	CacheLogger.Requests = make([]string, 0)
	assert.Equal(t, "Name 3", found[0].(TestEntityByIdsRedisCache).GetReferenceOne().Name)
	assert.Equal(t, "Name 4", found[1].(TestEntityByIdsRedisCache).GetReferenceOne().Name)
	assert.Equal(t, "Name 5", found[2].(TestEntityByIdsRedisCache).GetReferenceOne().Name)
	assert.Equal(t, "Name 5", found[0].(TestEntityByIdsRedisCache).GetReferenceTwo().Name)
	assert.Equal(t, "Name 6", found[1].(TestEntityByIdsRedisCache).GetReferenceTwo().Name)
	assert.Equal(t, "Name 7", found[2].(TestEntityByIdsRedisCache).GetReferenceTwo().Name)
	assert.Equal(t, "Name 4", found[0].(TestEntityByIdsRedisCache).GetReferenceTwo().GetReferenceOne().Name)
	assert.Equal(t, "Name 5", found[1].(TestEntityByIdsRedisCache).GetReferenceTwo().GetReferenceOne().Name)
	assert.Equal(t, "Name 2", found[2].(TestEntityByIdsRedisCache).GetReferenceTwo().GetReferenceOne().Name)
	assert.Len(t, DBLogger.Queries, 4)

}

func BenchmarkGetByIdsRedis(b *testing.B) {
	var entity TestEntityByIdsRedisCache
	PrepareTables(entity)

	_ = orm.Flush(&TestEntityByIdsRedisCache{Name: "Hi 1"}, &TestEntityByIdsRedisCache{Name: "Hi 2"}, &TestEntityByIdsRedisCache{Name: "Hi 3"})

	for n := 0; n < b.N; n++ {
		_, _ = orm.TryByIds([]uint64{1, 2, 3}, TestEntityByIdsRedisCacheName)
	}
}
