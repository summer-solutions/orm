package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"testing"
)

type TestEntityFlusherInCacheRedis struct {
	Orm  orm.ORM `orm:"table=TestEntityFlushInCacheRedis;mysql=default;redisCache"`
	Id   uint
	Name string
}

type TestEntityFlusherInCacheLocal struct {
	Orm orm.ORM `orm:"table=TestEntityFlushInCacheLocal;mysql=default"`
	Id  uint
}

func TestFlushInCache(t *testing.T) {

	entityRedis := TestEntityFlusherInCacheRedis{Name: "Name"}
	entityLocal := TestEntityFlusherInCacheLocal{}
	PrepareTables(entityRedis, entityLocal)

	err := orm.Flush(&entityRedis)
	assert.Nil(t, err)

	LoggerDB := TestDatabaseLogger{}
	orm.GetMysqlDB("default").AddLogger(&LoggerDB)
	LoggerRedisCache := TestCacheLogger{}
	orm.GetRedisCache("default").AddLogger(&LoggerRedisCache)
	LoggerRedisQueue := TestCacheLogger{}
	orm.GetRedisCache("default_queue").AddLogger(&LoggerRedisQueue)

	orm.GetMysqlDB("default").AddLogger(orm.StandardDatabaseLogger{})
	orm.GetRedisCache("default").AddLogger(orm.StandardCacheLogger{})
	orm.GetRedisCache("default_queue").AddLogger(orm.StandardCacheLogger{})
	orm.GetLocalCacheContainer("default").AddLogger(orm.StandardCacheLogger{})

	entityRedis.Name = "Name 2"

	err = orm.FlushInCache(&entityLocal, &entityRedis)
	assert.Nil(t, err)
	assert.Len(t, LoggerDB.Queries, 1)
	assert.Equal(t, "INSERT INTO TestEntityFlushInCacheLocal() VALUES () []", LoggerDB.Queries[0])
	assert.Len(t, LoggerRedisCache.Requests, 1)
	assert.Equal(t, "MSET [TestEntityFlushInCacheRedis49:1 ] ", LoggerRedisCache.Requests[0])
	assert.Len(t, LoggerRedisQueue.Requests, 1)
	assert.Equal(t, "ZADD 1 values dirty_queue", LoggerRedisQueue.Requests[0])

}
