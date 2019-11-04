package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"strconv"
	"testing"
)

const TestEntityFlushName = "tests.TestEntityFlush"

type TestEntityFlush struct {
	Orm  orm.ORM `orm:"table=TestFlush;mysql=default"`
	Id   uint
	Name string
}

const TestEntityFlushCacheLocalName = "tests.TestEntityFlushCacheLocal"

type TestEntityFlushCacheLocal struct {
	Orm  orm.ORM `orm:"table=TestFlushCache;mysql=default;localCache"`
	Id   uint
	Name string
}

const TestEntityFlushCacheRedisName = "tests.TestEntityFlushCacheRedis"

type TestEntityFlushCacheRedis struct {
	Orm  orm.ORM `orm:"table=TestFlushCache;mysql=default;redisCache"`
	Id   uint
	Name string
}

func TestFlush(t *testing.T) {
	var entity TestEntityFlush
	PrepareTables(entity)

	var entities = make([]interface{}, 10)
	for i := 1; i <= 10; i++ {
		e := TestEntityFlush{Name: "Name " + strconv.Itoa(i)}
		entities[i-1] = &e
	}
	err := orm.Flush(entities...)
	assert.Nil(t, err)
	for i := 1; i < 10; i++ {
		testEntity := entities[i-1].(*TestEntityFlush)
		assert.Equal(t, uint(i), testEntity.Id)
		assert.Equal(t, "Name "+strconv.Itoa(i), testEntity.Name)
		dirty, _ := orm.IsDirty(testEntity)
		assert.False(t, dirty)
	}

	entities[1].(*TestEntityFlush).Name = "Name 2.1"
	entities[7].(*TestEntityFlush).Name = "Name 8.1"
	dirty, _ := orm.IsDirty(entities[1])
	assert.True(t, dirty)
	dirty, _ = orm.IsDirty(entities[7])
	assert.True(t, dirty)
	err = orm.Flush(entities...)
	assert.Nil(t, err)
	edited1 := orm.GetById(2, TestEntityFlushName)
	edited2 := orm.GetById(8, TestEntityFlushName)
	assert.Equal(t, "Name 2.1", edited1.(TestEntityFlush).Name)
	assert.Equal(t, "Name 8.1", edited2.(TestEntityFlush).Name)

	edited := edited1.(TestEntityFlush)
	toDelete := edited2.(TestEntityFlush)
	edited.Name = "Name 2.2"
	orm.MarkToDelete(&toDelete)
	newEntity := TestEntityFlush{Name: "Name 11"}
	dirty, _ = orm.IsDirty(edited)
	assert.True(t, dirty)
	dirty, _ = orm.IsDirty(edited2)
	assert.True(t, dirty)
	dirty, _ = orm.IsDirty(newEntity)
	assert.True(t, dirty)

	err = orm.Flush(&edited, &newEntity, &toDelete)
	assert.Nil(t, err)

	dirty, _ = orm.IsDirty(edited)
	assert.False(t, dirty)
	dirty, _ = orm.IsDirty(newEntity)
	assert.False(t, dirty)
	dirty, _ = orm.IsDirty(newEntity)
	assert.False(t, dirty)

	edited3 := orm.GetById(2, TestEntityFlushName)
	deleted, hasDeleted := orm.TryById(8, TestEntityFlushName)
	new11 := orm.GetById(11, TestEntityFlushName)
	assert.Equal(t, "Name 2.2", edited3.(TestEntityFlush).Name)
	assert.False(t, hasDeleted)
	assert.Nil(t, deleted)
	assert.Equal(t, "Name 11", new11.(TestEntityFlush).Name)
}

func TestFlushTransactionLocalCache(t *testing.T) {

	entity := TestEntityFlushCacheLocal{Name: "Name"}
	PrepareTables(entity)

	DbLogger := TestDatabaseLogger{}
	CacheLogger := TestCacheLogger{}
	orm.GetMysqlDB("default").AddLogger(&DbLogger)
	orm.GetLocalCacheContainer("default").AddLogger(&CacheLogger)

	orm.GetTableSchema(TestEntityFlushCacheLocalName).GetMysqlDB().BeginTransaction()
	err := orm.Flush(&entity)
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 0)
	err = orm.GetTableSchema(TestEntityFlushCacheLocalName).GetMysqlDB().Commit()
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 1)
	assert.Equal(t, "MSET [TestFlushCache49:1]", CacheLogger.Requests[0])

	orm.GetTableSchema(TestEntityFlushCacheLocalName).GetMysqlDB().BeginTransaction()
	err = orm.Flush(&entity)
	assert.Nil(t, err)
	entity.Name = "New Name"
	err = orm.Flush(&entity)
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 1)
	err = orm.GetTableSchema(TestEntityFlushCacheLocalName).GetMysqlDB().Commit()
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 2)
	assert.Equal(t, "MSET [TestFlushCache49:1]", CacheLogger.Requests[0])

	orm.GetTableSchema(TestEntityFlushCacheLocalName).GetMysqlDB().BeginTransaction()
	orm.MarkToDelete(entity)
	err = orm.Flush(&entity)
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 2)
	err = orm.GetTableSchema(TestEntityFlushCacheLocalName).GetMysqlDB().Commit()
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 3)
	assert.Equal(t, "MSET [TestFlushCache49:1]", CacheLogger.Requests[0])
}

func TestFlushTransactionRedisCache(t *testing.T) {

	entity := TestEntityFlushCacheRedis{Name: "Name"}
	PrepareTables(entity)

	DbLogger := TestDatabaseLogger{}
	CacheLogger := TestCacheLogger{}
	orm.GetMysqlDB("default").AddLogger(&DbLogger)
	orm.GetRedisCache("default").AddLogger(&CacheLogger)

	orm.GetTableSchema(TestEntityFlushCacheRedisName).GetMysqlDB().BeginTransaction()
	err := orm.Flush(&entity)
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 0)
	err = orm.GetTableSchema(TestEntityFlushCacheRedisName).GetMysqlDB().Commit()
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 1)
	assert.Equal(t, "DELETE TestFlushCache49:1", CacheLogger.Requests[0])

	orm.GetTableSchema(TestEntityFlushCacheRedisName).GetMysqlDB().BeginTransaction()
	err = orm.Flush(&entity)
	assert.Nil(t, err)
	entity.Name = "New Name"
	err = orm.Flush(&entity)
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 1)
	err = orm.GetTableSchema(TestEntityFlushCacheRedisName).GetMysqlDB().Commit()
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 2)
	assert.Equal(t, "DELETE TestFlushCache49:1", CacheLogger.Requests[0])

	orm.GetTableSchema(TestEntityFlushCacheRedisName).GetMysqlDB().BeginTransaction()
	orm.MarkToDelete(entity)
	err = orm.Flush(&entity)
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 2)
	err = orm.GetTableSchema(TestEntityFlushCacheRedisName).GetMysqlDB().Commit()
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 3)
	assert.Equal(t, "DELETE TestFlushCache49:1", CacheLogger.Requests[0])
}
