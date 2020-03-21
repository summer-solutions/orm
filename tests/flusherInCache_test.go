package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"testing"
)

type TestEntityFlusherInCacheRedis struct {
	Orm      *orm.ORM `orm:"redisCache"`
	Id       uint
	Name     string
	Age      uint16
	IndexAge *orm.CachedQuery `query:":Age = ? ORDER BY :Id"`
}

type TestEntityFlusherInCacheLocal struct {
	Orm *orm.ORM
	Id  uint
}

func TestFlushInCache(t *testing.T) {

	entityRedis := TestEntityFlusherInCacheRedis{Name: "Name", Age: 18}
	entityLocal := TestEntityFlusherInCacheLocal{}
	PrepareTables(entityRedis, entityLocal)

	err := orm.Flush(&entityRedis)
	assert.Nil(t, err)

	pager := &orm.Pager{CurrentPage: 1, PageSize: 100}
	var rows []*TestEntityFlusherInCacheRedis
	totalRows, err := orm.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Nil(t, err)
	assert.Equal(t, 1, totalRows)
	assert.Len(t, rows, 1)
	totalRows, err = orm.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Nil(t, err)
	assert.Equal(t, 0, totalRows)
	assert.Len(t, rows, 0)

	DBLogger := &TestDatabaseLogger{}
	orm.GetMysql().RegisterLogger(DBLogger.Logger())
	LoggerRedisCache := &TestCacheLogger{}
	orm.GetRedis().RegisterLogger(LoggerRedisCache.Logger())
	LoggerRedisQueue := &TestCacheLogger{}
	orm.GetRedis("default_queue").RegisterLogger(LoggerRedisQueue.Logger())

	entityRedis.Name = "Name 2"
	entityRedis.Age = 10

	err = orm.FlushInCache(&entityLocal, &entityRedis)
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Queries, 1)
	assert.Equal(t, "INSERT INTO TestEntityFlusherInCacheLocal() VALUES () []", DBLogger.Queries[0])
	assert.Len(t, LoggerRedisCache.Requests, 1)
	assert.Equal(t, "MSET [TestEntityFlusherInCacheRedis3c:1 ] ", LoggerRedisCache.Requests[0])
	assert.Len(t, LoggerRedisQueue.Requests, 1)
	assert.Equal(t, "SADD 1 values dirty_queue", LoggerRedisQueue.Requests[0])

	var loadedEntity TestEntityFlusherInCacheRedis
	err = orm.GetById(1, &loadedEntity)
	assert.Nil(t, err)
	assert.Equal(t, "Name 2", loadedEntity.Name)
	assert.Len(t, LoggerRedisCache.Requests, 2)
	assert.Equal(t, "GET TestEntityFlusherInCacheRedis3c:1", LoggerRedisCache.Requests[1])

	receiver := orm.FlushFromCacheReceiver{QueueName: "default"}
	size, err := receiver.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(1), size)

	has, err := receiver.Digest()
	assert.Nil(t, err)
	assert.True(t, has)
	has, err = receiver.Digest()
	assert.Nil(t, err)
	assert.False(t, has)
	size, err = receiver.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), size)

	var inDB TestEntityFlusherInCacheRedis
	_, err = orm.SearchOne(orm.NewWhere("`Id` = ?", 1), &inDB)
	assert.Nil(t, err)
	assert.Equal(t, "Name 2", inDB.Name)

	totalRows, err = orm.CachedSearch(&rows, "IndexAge", pager, 18)
	assert.Nil(t, err)
	assert.Equal(t, 0, totalRows)
	assert.Len(t, rows, 0)
	totalRows, err = orm.CachedSearch(&rows, "IndexAge", pager, 10)
	assert.Nil(t, err)
	assert.Equal(t, 1, totalRows)
	assert.Len(t, rows, 1)

}
