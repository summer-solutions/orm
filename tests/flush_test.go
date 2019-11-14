package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"strconv"
	"testing"
)

type TestEntityFlush struct {
	Orm           *orm.ORM `orm:"mysql=default"`
	Id            uint16
	Name          string
	ReferenceOne  *orm.ReferenceOne  `orm:"ref=tests.TestEntityFlush"`
	ReferenceMany *orm.ReferenceMany `orm:"ref=tests.TestEntityFlush"`
}

type TestEntityFlushCacheLocal struct {
	Orm  *orm.ORM `orm:"mysql=default;localCache"`
	Id   uint
	Name string
}

type TestEntityFlushCacheRedis struct {
	Orm  *orm.ORM `orm:"mysql=default;redisCache"`
	Id   uint
	Name string
}

func TestFlush(t *testing.T) {
	var entity TestEntityFlush
	PrepareTables(entity)

	var entities = make([]*TestEntityFlush, 10)
	flusher := orm.NewFlusher(100, false)
	for i := 1; i <= 10; i++ {
		e := TestEntityFlush{Name: "Name " + strconv.Itoa(i)}
		flusher.RegisterEntity(&e)
		entities[i-1] = &e
	}
	err := flusher.Flush()
	assert.Nil(t, err)
	for i := 1; i < 10; i++ {
		testEntity := entities[i-1]
		assert.Equal(t, uint16(i), testEntity.Id)
		assert.Equal(t, "Name "+strconv.Itoa(i), testEntity.Name)
		dirty := testEntity.Orm.IsDirty()
		assert.False(t, dirty)
	}

	entities[1].Name = "Name 2.1"
	entities[1].ReferenceOne.Id = 7
	entities[7].Name = "Name 8.1"
	entities[7].ReferenceMany.Add(3, 4)
	dirty := entities[1].Orm.IsDirty()
	assert.True(t, dirty)
	dirty = entities[7].Orm.IsDirty()
	assert.True(t, dirty)
	err = orm.Flush(entities[1], entities[7])
	assert.Nil(t, err)

	var edited1 TestEntityFlush
	var edited2 TestEntityFlush
	orm.GetById(2, &edited1)
	orm.GetById(8, &edited2)
	assert.Equal(t, "Name 2.1", edited1.Name)
	assert.Equal(t, uint64(7), edited1.ReferenceOne.Id)
	assert.Equal(t, "Name 8.1", edited2.Name)
	assert.Equal(t, uint64(0), edited2.ReferenceOne.Id)
	assert.True(t, edited1.ReferenceOne.Has())
	assert.False(t, edited2.ReferenceOne.Has())
	assert.Equal(t, 0, edited1.ReferenceMany.Len())
	assert.Equal(t, 2, edited2.ReferenceMany.Len())
	assert.Equal(t, []uint64{3, 4}, edited2.ReferenceMany.Ids)
	var refs []TestEntityFlush
	edited2.ReferenceMany.Load(&refs)
	assert.Len(t, refs, 2)
	assert.Equal(t, uint16(3), refs[0].Id)
	assert.Equal(t, uint16(4), refs[1].Id)

	var ref TestEntityFlush
	has := edited1.ReferenceOne.Load(&ref)
	assert.True(t, has)
	assert.Equal(t, uint16(7), ref.Id)

	toDelete := edited2
	edited1.Name = "Name 2.2"
	toDelete.Orm.MarkToDelete()
	newEntity := TestEntityFlush{Name: "Name 11"}
	orm.Init(&newEntity)
	dirty = edited1.Orm.IsDirty()
	assert.True(t, dirty)
	dirty = edited2.Orm.IsDirty()
	assert.True(t, dirty)
	dirty = newEntity.Orm.IsDirty()
	assert.True(t, dirty)

	err = orm.Flush(&edited1, &newEntity, &toDelete)
	assert.Nil(t, err)

	dirty = edited1.Orm.IsDirty()
	assert.False(t, dirty)
	dirty = newEntity.Orm.IsDirty()
	assert.False(t, dirty)
	dirty = newEntity.Orm.IsDirty()
	assert.False(t, dirty)

	var edited3 TestEntityFlush
	var deleted TestEntityFlush
	var new11 TestEntityFlush
	orm.GetById(2, &edited3)
	hasDeleted := orm.TryById(8, &deleted)
	orm.GetById(11, &new11)
	assert.Equal(t, "Name 2.2", edited3.Name)
	assert.False(t, hasDeleted)
	assert.Equal(t, "Name 11", new11.Name)
}

func TestFlushTransactionLocalCache(t *testing.T) {

	entity := TestEntityFlushCacheLocal{Name: "Name"}
	PrepareTables(entity)

	DbLogger := TestDatabaseLogger{}
	CacheLogger := TestCacheLogger{}
	orm.GetMysqlDB("default").AddLogger(&DbLogger)
	orm.GetLocalCacheContainer("default").AddLogger(&CacheLogger)

	orm.GetMysqlDB("default").BeginTransaction()
	err := orm.Flush(&entity)
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 0)
	err = orm.GetMysqlDB("default").Commit()
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 1)
	assert.Equal(t, "MSET [TestEntityFlushCacheLocal49:1]", CacheLogger.Requests[0])

	orm.GetMysqlDB("default").BeginTransaction()
	err = orm.Flush(&entity)
	assert.Nil(t, err)
	entity.Name = "New Name"
	err = orm.Flush(&entity)
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 1)
	err = orm.GetMysqlDB("default").Commit()
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 2)
	assert.Equal(t, "MSET [TestEntityFlushCacheLocal49:1]", CacheLogger.Requests[0])

	orm.GetMysqlDB("default").BeginTransaction()
	entity.Orm.MarkToDelete()
	err = orm.Flush(&entity)
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 2)
	err = orm.GetMysqlDB("default").Commit()
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 3)
	assert.Equal(t, "MSET [TestEntityFlushCacheLocal49:1]", CacheLogger.Requests[0])
}

func TestFlushTransactionRedisCache(t *testing.T) {

	entity := TestEntityFlushCacheRedis{Name: "Name"}
	PrepareTables(entity)

	DbLogger := TestDatabaseLogger{}
	CacheLogger := TestCacheLogger{}
	orm.GetMysqlDB("default").AddLogger(&DbLogger)
	orm.GetRedisCache("default").AddLogger(&CacheLogger)

	orm.GetMysqlDB("default").BeginTransaction()
	err := orm.Flush(&entity)
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 0)
	err = orm.GetMysqlDB("default").Commit()
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 1)
	assert.Equal(t, "DELETE TestEntityFlushCacheRedis49:1", CacheLogger.Requests[0])

	orm.GetMysqlDB("default").BeginTransaction()
	err = orm.Flush(&entity)
	assert.Nil(t, err)
	entity.Name = "New Name"
	err = orm.Flush(&entity)
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 1)
	err = orm.GetMysqlDB("default").Commit()
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 2)
	assert.Equal(t, "DELETE TestEntityFlushCacheRedis49:1", CacheLogger.Requests[0])

	orm.GetMysqlDB("default").BeginTransaction()
	entity.Orm.MarkToDelete()
	err = orm.Flush(&entity)
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 2)
	err = orm.GetMysqlDB("default").Commit()
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 3)
	assert.Equal(t, "DELETE TestEntityFlushCacheRedis49:1", CacheLogger.Requests[0])
}
