package tests

import (
	"github.com/go-redis/redis/v7"
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"testing"
)

type TestEntityDirtyQueueAll struct {
	Orm  *orm.ORM `orm:"mysql=default;dirty=test"`
	Id   uint
	Name string
}

type TestEntityDirtyQueueAge struct {
	Orm  *orm.ORM `orm:"mysql=default"`
	Id   uint
	Name string
	Age  uint16 `orm:"dirty=test"`
}

func TestDirtyQueue(t *testing.T) {

	entityAll := TestEntityDirtyQueueAll{Name: "Name"}
	entityAge := TestEntityDirtyQueueAge{Name: "Name", Age: 18}
	PrepareTables(entityAll, entityAge)
	orm.RegisterDirtyQueue("test", "default_queue")

	LoggerRedisQueue := TestCacheLogger{}
	orm.GetRedisCache("default_queue").AddLogger(&LoggerRedisQueue)

	err := orm.Flush(&entityAll, &entityAge)
	assert.Nil(t, err)
	assert.Len(t, LoggerRedisQueue.Requests, 1)
	assert.Equal(t, "ZADD 2 values test", LoggerRedisQueue.Requests[0])

	receiver := orm.DirtyReceiver{QueueCode: "test"}
	assert.Equal(t, int64(2), receiver.Size())
	err = receiver.Digest(2, func(data []orm.DirtyData) (invalid []*redis.Z, err error) {
		assert.Len(t, data, 2)
		assert.Equal(t, "TestEntityDirtyQueueAge", data[0].TableSchema.TableName)
		assert.Equal(t, "TestEntityDirtyQueueAll", data[1].TableSchema.TableName)
		assert.Equal(t, uint64(1), data[0].Id)
		assert.Equal(t, uint64(1), data[1].Id)
		assert.True(t, data[0].Inserted)
		assert.True(t, data[1].Inserted)
		assert.False(t, data[0].Updated)
		assert.False(t, data[1].Updated)
		assert.False(t, data[0].Deleted)
		assert.False(t, data[1].Deleted)
		return nil, nil
	})
	assert.Nil(t, err)
	assert.Equal(t, int64(0), receiver.Size())

	entityAll.Name = "Name 2"
	err = orm.Flush(&entityAll)
	assert.Nil(t, err)
	assert.Len(t, LoggerRedisQueue.Requests, 6)
	assert.Equal(t, "ZADD 1 values test", LoggerRedisQueue.Requests[5])

	entityAge.Name = "Name 2"
	err = orm.Flush(&entityAll)
	assert.Nil(t, err)
	assert.Len(t, LoggerRedisQueue.Requests, 6)

	assert.Equal(t, int64(1), receiver.Size())
	err = receiver.Digest(100, func(data []orm.DirtyData) (invalid []*redis.Z, err error) {
		assert.Len(t, data, 1)
		assert.Equal(t, "TestEntityDirtyQueueAll", data[0].TableSchema.TableName)
		assert.Equal(t, uint64(1), data[0].Id)
		assert.False(t, data[0].Inserted)
		assert.True(t, data[0].Updated)
		assert.False(t, data[0].Deleted)
		return nil, nil
	})
	assert.Nil(t, err)
	assert.Equal(t, int64(0), receiver.Size())

	entityAge.Age = 10
	err = orm.Flush(&entityAge)
	assert.Nil(t, err)
	assert.Len(t, LoggerRedisQueue.Requests, 11)
	assert.Equal(t, "ZADD 1 values test", LoggerRedisQueue.Requests[10])

	assert.Equal(t, int64(1), receiver.Size())
	err = receiver.Digest(100, func(data []orm.DirtyData) (invalid []*redis.Z, err error) {
		assert.Len(t, data, 1)
		assert.Equal(t, "TestEntityDirtyQueueAge", data[0].TableSchema.TableName)
		assert.Equal(t, uint64(1), data[0].Id)
		assert.False(t, data[0].Inserted)
		assert.True(t, data[0].Updated)
		assert.False(t, data[0].Deleted)
		return nil, nil
	})
	assert.Nil(t, err)
	assert.Equal(t, int64(0), receiver.Size())

	entityAge.Orm.MarkToDelete()
	err = orm.Flush(&entityAge)
	assert.Nil(t, err)

	assert.Equal(t, int64(1), receiver.Size())
	err = receiver.Digest(100, func(data []orm.DirtyData) (invalid []*redis.Z, err error) {
		assert.Len(t, data, 1)
		assert.Equal(t, "TestEntityDirtyQueueAge", data[0].TableSchema.TableName)
		assert.Equal(t, uint64(1), data[0].Id)
		assert.False(t, data[0].Inserted)
		assert.False(t, data[0].Updated)
		assert.True(t, data[0].Deleted)
		return nil, nil
	})
	assert.Nil(t, err)
	assert.Equal(t, int64(0), receiver.Size())
}
