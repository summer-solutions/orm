package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"strconv"
	"testing"
	"time"
)

type TestEntityFlush struct {
	Orm          *orm.ORM
	Id           uint16
	Name         string
	NameNotNull  string `orm:"required"`
	Blob         []byte
	Enum         string `orm:"enum=tests.Color"`
	EnumNotNull  string `orm:"enum=tests.Color;required"`
	Year         uint16 `orm:"year=true"`
	YearNotNull  uint16 `orm:"year=true;required"`
	Date         time.Time
	DateNotNull  time.Time         `orm:"required"`
	ReferenceOne *orm.ReferenceOne `orm:"ref=tests.TestEntityFlush"`
	Ignored      []time.Time       `orm:"ignore"`
}

type TestEntityFlushCacheLocal struct {
	Orm  *orm.ORM `orm:"localCache"`
	Id   uint
	Name string
}

type TestEntityFlushCacheRedis struct {
	Orm  *orm.ORM `orm:"redisCache"`
	Id   uint
	Name string
}

func TestFlush(t *testing.T) {
	orm.RegisterEnum("tests.Color", Color)
	var entity TestEntityFlush
	PrepareTables(entity)

	var entities = make([]*TestEntityFlush, 10)
	flusher := orm.Flusher{}
	for i := 1; i <= 10; i++ {
		e := TestEntityFlush{Name: "Name " + strconv.Itoa(i), EnumNotNull: Color.Red}
		flusher.RegisterEntity(&e)
		entities[i-1] = &e
	}
	err := flusher.Flush()
	assert.Nil(t, err)
	for i := 1; i < 10; i++ {
		testEntity := entities[i-1]
		assert.Equal(t, uint16(i), testEntity.Id)
		assert.Equal(t, "Name "+strconv.Itoa(i), testEntity.Name)
		assert.False(t, testEntity.Orm.IsDirty())
	}

	entities[1].Name = "Name 2.1"
	entities[1].ReferenceOne.Id = 7
	entities[7].Name = "Name 8.1"
	assert.Nil(t, err)
	assert.True(t, entities[1].Orm.IsDirty())
	assert.True(t, entities[7].Orm.IsDirty())
	err = orm.Flush(entities[1], entities[7])
	assert.Nil(t, err)

	var edited1 TestEntityFlush
	var edited2 TestEntityFlush
	err = orm.GetById(2, &edited1)
	assert.Nil(t, err)
	err = orm.GetById(8, &edited2)
	assert.Nil(t, err)
	assert.Equal(t, "Name 2.1", edited1.Name)
	assert.Equal(t, uint64(7), edited1.ReferenceOne.Id)
	assert.Equal(t, "Name 8.1", edited2.Name)
	assert.Equal(t, uint64(0), edited2.ReferenceOne.Id)
	assert.True(t, edited1.ReferenceOne.Has())
	assert.False(t, edited2.ReferenceOne.Has())
	var ref TestEntityFlush
	has, err := edited1.ReferenceOne.Load(&ref)
	assert.Nil(t, err)
	assert.True(t, has)
	assert.Equal(t, uint16(7), ref.Id)

	toDelete := edited2
	edited1.Name = "Name 2.2"
	toDelete.Orm.MarkToDelete()
	newEntity := TestEntityFlush{Name: "Name 11", EnumNotNull: Color.Red}
	orm.Init(&newEntity)
	assert.True(t, edited1.Orm.IsDirty())
	assert.Nil(t, err)
	assert.True(t, edited2.Orm.IsDirty())
	assert.Nil(t, err)
	assert.True(t, newEntity.Orm.IsDirty())

	err = orm.Flush(&edited1, &newEntity, &toDelete)
	assert.Nil(t, err)

	assert.False(t, edited1.Orm.IsDirty())
	assert.False(t, newEntity.Orm.IsDirty())
	assert.False(t, newEntity.Orm.IsDirty())

	var edited3 TestEntityFlush
	var deleted TestEntityFlush
	var new11 TestEntityFlush
	err = orm.GetById(2, &edited3)
	assert.Nil(t, err)
	hasDeleted, err := orm.TryById(8, &deleted)
	assert.Nil(t, err)
	err = orm.GetById(11, &new11)
	assert.Nil(t, err)
	assert.Equal(t, "Name 2.2", edited3.Name)
	assert.False(t, hasDeleted)
	assert.Equal(t, "Name 11", new11.Name)
}

func TestFlushTransactionLocalCache(t *testing.T) {

	entity := TestEntityFlushCacheLocal{Name: "Name"}
	PrepareTables(entity)

	DBLogger := &TestDatabaseLogger{}
	orm.GetMysql().RegisterLogger(DBLogger.Logger())
	CacheLogger := &TestCacheLogger{}
	orm.GetLocalCache().RegisterLogger(CacheLogger.Logger())

	err := orm.GetMysql().BeginTransaction()
	assert.Nil(t, err)
	err = orm.Flush(&entity)
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 0)
	err = orm.GetMysql().Commit()
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 1)
	assert.Equal(t, "MSET [TestEntityFlushCacheLocal49:1]", CacheLogger.Requests[0])

	err = orm.GetMysql().BeginTransaction()
	assert.Nil(t, err)
	err = orm.Flush(&entity)
	assert.Nil(t, err)
	entity.Name = "New Name"
	err = orm.Flush(&entity)
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 1)
	err = orm.GetMysql().Commit()
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 2)
	assert.Equal(t, "MSET [TestEntityFlushCacheLocal49:1]", CacheLogger.Requests[0])

	err = orm.GetMysql().BeginTransaction()
	assert.Nil(t, err)
	entity.Orm.MarkToDelete()
	err = orm.Flush(&entity)
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 2)
	err = orm.GetMysql().Commit()
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 3)
	assert.Equal(t, "MSET [TestEntityFlushCacheLocal49:1]", CacheLogger.Requests[0])
}

func TestFlushTransactionRedisCache(t *testing.T) {

	entity := TestEntityFlushCacheRedis{Name: "Name"}
	PrepareTables(entity)

	DBLogger := &TestDatabaseLogger{}
	orm.GetMysql().RegisterLogger(DBLogger.Logger())
	CacheLogger := &TestCacheLogger{}
	orm.GetRedis().RegisterLogger(CacheLogger.Logger())

	err := orm.GetMysql().BeginTransaction()
	assert.Nil(t, err)
	err = orm.Flush(&entity)
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 0)
	err = orm.GetMysql().Commit()
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 1)
	assert.Equal(t, "DELETE TestEntityFlushCacheRedis49:1", CacheLogger.Requests[0])

	err = orm.GetMysql().BeginTransaction()
	assert.Nil(t, err)
	err = orm.Flush(&entity)
	assert.Nil(t, err)
	entity.Name = "New Name"
	err = orm.Flush(&entity)
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 1)
	err = orm.GetMysql().Commit()
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 2)
	assert.Equal(t, "DELETE TestEntityFlushCacheRedis49:1", CacheLogger.Requests[0])

	err = orm.GetMysql().BeginTransaction()
	assert.Nil(t, err)
	entity.Orm.MarkToDelete()
	err = orm.Flush(&entity)
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 2)
	err = orm.GetMysql().Commit()
	assert.Nil(t, err)
	assert.Len(t, CacheLogger.Requests, 3)
	assert.Equal(t, "DELETE TestEntityFlushCacheRedis49:1", CacheLogger.Requests[0])
}
