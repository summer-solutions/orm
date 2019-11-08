package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"testing"
)

const TestEntityFlusherInCacheRedisName = "tests.TestEntityFlusherInCacheRedis"

type TestEntityFlusherInCacheRedis struct {
	Orm      orm.ORM `orm:"table=TestEntityFlushInCacheRedis;mysql=default;redisCache"`
	Id       uint
	Name     string
	Age      uint16
	IndexAge orm.CachedQuery `query:":Age = ? ORDER BY :Id"`
}

type TestEntityFlusherInCacheLocal struct {
	Orm orm.ORM `orm:"table=TestEntityFlushInCacheLocal;mysql=default"`
	Id  uint
}

func TestFlushInCache(t *testing.T) {

	entityRedis := TestEntityFlusherInCacheRedis{Name: "Name", Age: 18}
	entityLocal := TestEntityFlusherInCacheLocal{}
	PrepareTables(entityRedis, entityLocal)

	err := orm.Flush(&entityRedis)
	assert.Nil(t, err)

	pager := orm.NewPager(1, 100)
	rows, totalRows := orm.CachedSearch(TestEntityFlusherInCacheRedisName, "IndexAge", pager, 18)
	assert.Equal(t, 1, totalRows)
	assert.Len(t, rows, 1)
	rows, totalRows = orm.CachedSearch(TestEntityFlusherInCacheRedisName, "IndexAge", pager, 10)
	assert.Equal(t, 0, totalRows)
	assert.Len(t, rows, 0)

	LoggerDB := TestDatabaseLogger{}
	orm.GetMysqlDB("default").AddLogger(&LoggerDB)
	LoggerRedisCache := TestCacheLogger{}
	orm.GetRedisCache("default").AddLogger(&LoggerRedisCache)
	LoggerRedisQueue := TestCacheLogger{}
	orm.GetRedisCache("default_queue").AddLogger(&LoggerRedisQueue)

	entityRedis.Name = "Name 2"
	entityRedis.Age = 10

	err = orm.FlushInCache(&entityLocal, &entityRedis)
	assert.Nil(t, err)
	assert.Len(t, LoggerDB.Queries, 1)
	assert.Equal(t, "INSERT INTO TestEntityFlushInCacheLocal() VALUES () []", LoggerDB.Queries[0])
	assert.Len(t, LoggerRedisCache.Requests, 1)
	assert.Equal(t, "MSET [TestEntityFlushInCacheRedis3c:1 ] ", LoggerRedisCache.Requests[0])
	assert.Len(t, LoggerRedisQueue.Requests, 1)
	assert.Equal(t, "ZADD 1 values dirty_queue", LoggerRedisQueue.Requests[0])

	loadedEntity := orm.GetById(1, TestEntityFlusherInCacheRedisName).(TestEntityFlusherInCacheRedis)
	assert.Equal(t, "Name 2", loadedEntity.Name)
	assert.Len(t, LoggerRedisCache.Requests, 2)
	assert.Equal(t, "GET TestEntityFlushInCacheRedis3c:1", LoggerRedisCache.Requests[1])

	receiver := orm.FlushInCacheReceiver{RedisName: "default_queue"}
	assert.Equal(t, int64(1), receiver.Size())

	err = receiver.Digest()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), receiver.Size())

	inDB := orm.SearchOne(orm.NewWhere("`Id` = ?", 1), orm.NewPager(1, 1), TestEntityFlusherInCacheRedisName)
	assert.Equal(t, "Name 2", inDB.(TestEntityFlusherInCacheRedis).Name)

	rows, totalRows = orm.CachedSearch(TestEntityFlusherInCacheRedisName, "IndexAge", pager, 18)
	assert.Equal(t, 0, totalRows)
	assert.Len(t, rows, 0)
	rows, totalRows = orm.CachedSearch(TestEntityFlusherInCacheRedisName, "IndexAge", pager, 10)
	assert.Equal(t, 1, totalRows)
	assert.Len(t, rows, 1)

}
