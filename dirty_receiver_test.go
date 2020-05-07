package orm

import (
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

func TestDirtyReceiver(t *testing.T) {
	entityAll := &testEntityDirtyQueueAll{Name: "Name"}
	entityAge := &testEntityDirtyQueueAge{Name: "Name", Age: 18}
	registry := &Registry{}
	registry.RegisterDirtyQueue("test", &RedisQueueSender{PoolName: "default_queue"})
	engine := PrepareTables(t, registry, entityAll, entityAge)

	LoggerRedisQueue := memory.New()
	cache := engine.GetRedis("default_queue")
	cache.AddLogger(LoggerRedisQueue)
	cache.SetLogLevel(log.InfoLevel)
	//engine.EnableDebug()

	engine.Track(entityAll)
	err := engine.Flush()
	assert.Nil(t, err)
	engine.Track(entityAge)
	err = engine.Flush()
	assert.Nil(t, err)

	receiver := NewDirtyReceiver(engine)

	queueName := "test"
	val, found, err := cache.RPop(queueName)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, val)
	err = receiver.Digest([]byte(val), func(data *DirtyData) error {
		assert.Equal(t, uint64(1), data.ID)
		assert.True(t, data.Added)
		assert.False(t, data.Updated)
		assert.False(t, data.Deleted)
		return nil
	})
	assert.Nil(t, err)
	val, found, err = cache.RPop(queueName)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, val)
	err = receiver.Digest([]byte(val), func(data *DirtyData) error {
		assert.Equal(t, uint64(1), data.ID)
		assert.True(t, data.Added)
		assert.False(t, data.Updated)
		assert.False(t, data.Deleted)
		return nil
	})
	assert.Nil(t, err)

	engine.Track(entityAll)
	entityAll.Name = "Name 2"
	err = engine.Flush()
	assert.Nil(t, err)

	val, found, err = cache.RPop(queueName)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, val)
	err = receiver.Digest([]byte(val), func(data *DirtyData) error {
		assert.Equal(t, "testEntityDirtyQueueAll", data.TableSchema.GetTableName())
		assert.Equal(t, uint64(1), data.ID)
		assert.False(t, data.Added)
		assert.True(t, data.Updated)
		assert.False(t, data.Deleted)
		return nil
	})
	assert.Nil(t, err)

	engine.Track(entityAge)
	entityAge.Age = 10
	err = engine.Flush()
	assert.Nil(t, err)

	val, found, err = cache.RPop(queueName)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, val)
	err = receiver.Digest([]byte(val), func(data *DirtyData) error {
		assert.Equal(t, "testEntityDirtyQueueAge", data.TableSchema.GetTableName())
		assert.Equal(t, uint64(1), data.ID)
		assert.False(t, data.Added)
		assert.True(t, data.Updated)
		assert.False(t, data.Deleted)
		return nil
	})
	assert.Nil(t, err)

	engine.MarkToDelete(entityAge)
	err = engine.Flush()
	assert.Nil(t, err)

	val, found, err = cache.RPop(queueName)
	assert.NoError(t, err)
	assert.True(t, found)
	assert.NotNil(t, val)
	err = receiver.Digest([]byte(val), func(data *DirtyData) error {
		assert.Equal(t, "testEntityDirtyQueueAge", data.TableSchema.GetTableName())
		assert.Equal(t, uint64(1), data.ID)
		assert.False(t, data.Added)
		assert.False(t, data.Updated)
		assert.True(t, data.Deleted)
		return nil
	})
	assert.Nil(t, err)
}
