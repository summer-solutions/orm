package orm

import (
	"fmt"
	"testing"

	"github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

type testEntityDirtyQueueAll struct {
	ORM  `orm:"dirty=test"`
	ID   uint
	Name string `orm:"length=100"`
}

type testEntityDirtyQueueAge struct {
	ORM
	ID   uint
	Name string `orm:"dirty=test"`
	Age  uint16 `orm:"dirty=test"`
}

func TestDirtyQueue(t *testing.T) {
	entityAll := &testEntityDirtyQueueAll{Name: "Name"}
	entityAge := &testEntityDirtyQueueAge{Name: "Name", Age: 18}
	registry := &Registry{}
	registry.RegisterDirtyQueue("test", &RedisDirtyQueueSender{PoolName: "default_queue"})
	engine := PrepareTables(t, registry, entityAll, entityAge)

	LoggerRedisQueue := memory.New()
	cache := engine.GetRedis("default_queue")
	cache.AddLogger(LoggerRedisQueue)
	cache.SetLogLevel(log.InfoLevel)

	engine.Track(entityAll)
	err := engine.Flush()
	assert.Nil(t, err)
	engine.Track(entityAge)
	err = engine.Flush()
	assert.Nil(t, err)

	assert.Len(t, LoggerRedisQueue.Entries, 2)
	assert.Equal(t, "[ORM][REDIS][SADD]", LoggerRedisQueue.Entries[0].Message)

	receiver := NewDirtyReceiver(engine, "test")

	entities := receiver.GetEntities()
	assert.Len(t, entities, 2)

	size, err := receiver.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(2), size)
	has, err := receiver.Digest(2, func(data []DirtyData) (invalid []interface{}, err error) {
		assert.Len(t, data, 2)
		assert.Equal(t, uint64(1), data[0].ID)
		assert.Equal(t, uint64(1), data[1].ID)
		assert.True(t, data[0].Inserted)
		assert.True(t, data[1].Inserted)
		assert.False(t, data[0].Updated)
		assert.False(t, data[1].Updated)
		assert.False(t, data[0].Deleted)
		assert.False(t, data[1].Deleted)
		return nil, nil
	})
	assert.Nil(t, err)
	assert.True(t, has)
	has, err = receiver.Digest(2, func(data []DirtyData) (invalid []interface{}, err error) {
		return nil, nil
	})
	assert.Nil(t, err)
	assert.False(t, has)
	size, err = receiver.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), size)

	engine.Track(entityAll)
	entityAll.Name = "Name 2"
	err = engine.Flush()
	assert.Nil(t, err)
	assert.Len(t, LoggerRedisQueue.Entries, 7)
	assert.Equal(t, "[ORM][REDIS][SADD]", LoggerRedisQueue.Entries[6].Message)

	size, err = receiver.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(1), size)
	has, err = receiver.Digest(100, func(data []DirtyData) (invalid []interface{}, err error) {
		assert.Len(t, data, 1)
		assert.Equal(t, "testEntityDirtyQueueAll", data[0].TableSchema.GetTableName())
		assert.Equal(t, uint64(1), data[0].ID)
		assert.False(t, data[0].Inserted)
		assert.True(t, data[0].Updated)
		assert.False(t, data[0].Deleted)
		return nil, nil
	})
	assert.Nil(t, err)
	assert.True(t, has)
	has, err = receiver.Digest(100, func(data []DirtyData) (invalid []interface{}, err error) {
		return nil, nil
	})
	assert.Nil(t, err)
	assert.False(t, has)
	size, err = receiver.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), size)

	engine.Track(entityAge)
	entityAge.Age = 10
	err = engine.Flush()
	assert.Nil(t, err)
	assert.Len(t, LoggerRedisQueue.Entries, 12)
	assert.Equal(t, "[ORM][REDIS][SADD]", LoggerRedisQueue.Entries[11].Message)

	size, err = receiver.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(1), size)
	has, err = receiver.Digest(100, func(data []DirtyData) (invalid []interface{}, err error) {
		assert.Len(t, data, 1)
		assert.Equal(t, "testEntityDirtyQueueAge", data[0].TableSchema.GetTableName())
		assert.Equal(t, uint64(1), data[0].ID)
		assert.False(t, data[0].Inserted)
		assert.True(t, data[0].Updated)
		assert.False(t, data[0].Deleted)
		return nil, nil
	})
	assert.Nil(t, err)
	assert.True(t, has)
	size, err = receiver.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), size)

	engine.MarkToDelete(entityAge)
	err = engine.Flush()
	assert.Nil(t, err)

	size, err = receiver.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(1), size)
	has, err = receiver.Digest(100, func(data []DirtyData) (invalid []interface{}, err error) {
		assert.Len(t, data, 1)
		assert.Equal(t, "testEntityDirtyQueueAge", data[0].TableSchema.GetTableName())
		assert.Equal(t, uint64(1), data[0].ID)
		assert.False(t, data[0].Inserted)
		assert.False(t, data[0].Updated)
		assert.True(t, data[0].Deleted)
		return nil, nil
	})
	assert.Nil(t, err)
	assert.True(t, has)
	size, err = receiver.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), size)

	err = receiver.MarkDirty("orm.testEntityDirtyQueueAge", 1, 2)
	assert.Nil(t, err)
	size, err = receiver.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(2), size)

	has, err = receiver.Digest(100, func(data []DirtyData) (invalid []interface{}, err error) {
		assert.Len(t, data, 2)
		assert.True(t, data[0].Updated)
		assert.True(t, data[1].Updated)
		assert.False(t, data[0].Inserted)
		assert.False(t, data[1].Inserted)
		assert.False(t, data[0].Deleted)
		assert.False(t, data[1].Deleted)
		return []interface{}{"a", "orm.testEntityDirtyQueueAge:u:f", "c:d:f"}, fmt.Errorf("has invalid")
	})
	assert.True(t, has)
	assert.NotNil(t, err)
	size, err = receiver.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(3), size)

	has, err = receiver.Digest(100, func(data []DirtyData) (invalid []interface{}, err error) {
		return nil, nil
	})
	assert.Nil(t, err)
	assert.True(t, has)
	size, err = receiver.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(0), size)

	r := engine.GetRedis("default_queue")
	mockClient := &mockRedisClient{client: r.client}
	r.client = mockClient
	mockClient.SPopNMock = func(key string, max int64) ([]string, error) {
		return nil, fmt.Errorf("redis error")
	}
	_, err = receiver.Digest(100, func(data []DirtyData) (invalid []interface{}, err error) {
		return nil, nil
	})
	assert.EqualError(t, err, "redis error")
}
