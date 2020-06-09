package orm

import (
	"testing"

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
	entityAll := &testEntityDirtyQueueAll{Name: "GetRabbitMQQueue"}
	entityAge := &testEntityDirtyQueueAge{Name: "Name", Age: 18}
	registry := &Registry{}
	registry.RegisterDirtyQueue("test", 2)
	engine := PrepareTables(t, registry, entityAll, entityAge)
	//engine.EnableQueryDebug()

	engine.Track(entityAll)
	engine.Flush()
	engine.Track(entityAge)
	engine.Flush()

	receiver := NewDirtyReceiver(engine)
	receiver.DisableLoop()
	queueName := "test"

	receiver.Digest(queueName, func(data []*DirtyData) {
		assert.Len(t, data, 2)
		assert.Equal(t, uint64(1), data[0].ID)
		assert.True(t, data[0].Added)
		assert.False(t, data[0].Updated)
		assert.False(t, data[0].Deleted)

		assert.Equal(t, uint64(1), data[1].ID)
		assert.True(t, data[1].Added)
		assert.False(t, data[1].Updated)
		assert.False(t, data[1].Deleted)
	})

	engine.Track(entityAll)
	entityAll.Name = "Name 2"
	engine.Flush()

	receiver.Digest(queueName, func(data []*DirtyData) {
		assert.Len(t, data, 1)
		assert.Equal(t, "testEntityDirtyQueueAll", data[0].TableSchema.GetTableName())
		assert.Equal(t, uint64(1), data[0].ID)
		assert.False(t, data[0].Added)
		assert.True(t, data[0].Updated)
		assert.False(t, data[0].Deleted)
	})

	engine.Track(entityAge)
	entityAge.Age = 10
	engine.Flush()

	receiver.Digest(queueName, func(data []*DirtyData) {
		assert.Len(t, data, 1)
		assert.Equal(t, "testEntityDirtyQueueAge", data[0].TableSchema.GetTableName())
		assert.Equal(t, uint64(1), data[0].ID)
		assert.False(t, data[0].Added)
		assert.True(t, data[0].Updated)
		assert.False(t, data[0].Deleted)
	})

	engine.MarkToDelete(entityAge)
	engine.Flush()

	receiver.Digest(queueName, func(data []*DirtyData) {
		assert.Len(t, data, 1)
		assert.Equal(t, "testEntityDirtyQueueAge", data[0].TableSchema.GetTableName())
		assert.Equal(t, uint64(1), data[0].ID)
		assert.False(t, data[0].Added)
		assert.False(t, data[0].Updated)
		assert.True(t, data[0].Deleted)
	})
}
