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
	entityAll := &testEntityDirtyQueueAll{Name: "Name"}
	entityAge := &testEntityDirtyQueueAge{Name: "Name", Age: 18}
	registry := &Registry{}
	registry.RegisterDirtyQueue("test", 2)
	engine := PrepareTables(t, registry, entityAll, entityAge)

	//engine.EnableDebug()

	engine.Track(entityAll)
	err := engine.Flush()
	assert.Nil(t, err)
	engine.Track(entityAge)
	err = engine.Flush()
	assert.Nil(t, err)

	receiver := NewDirtyReceiver(engine)
	receiver.DisableLoop()
	queueName := "test"

	err = receiver.Digest(queueName, func(data []*DirtyData) error {
		assert.Len(t, data, 2)
		assert.Equal(t, uint64(1), data[0].ID)
		assert.True(t, data[0].Added)
		assert.False(t, data[0].Updated)
		assert.False(t, data[0].Deleted)

		assert.Equal(t, uint64(1), data[1].ID)
		assert.True(t, data[1].Added)
		assert.False(t, data[1].Updated)
		assert.False(t, data[1].Deleted)
		return nil
	})
	assert.Nil(t, err)

	engine.Track(entityAll)
	entityAll.Name = "Name 2"
	err = engine.Flush()
	assert.Nil(t, err)

	err = receiver.Digest(queueName, func(data []*DirtyData) error {
		assert.Len(t, data, 1)
		assert.Equal(t, "testEntityDirtyQueueAll", data[0].TableSchema.GetTableName())
		assert.Equal(t, uint64(1), data[0].ID)
		assert.False(t, data[0].Added)
		assert.True(t, data[0].Updated)
		assert.False(t, data[0].Deleted)
		return nil
	})
	assert.Nil(t, err)

	engine.Track(entityAge)
	entityAge.Age = 10
	err = engine.Flush()
	assert.Nil(t, err)

	err = receiver.Digest(queueName, func(data []*DirtyData) error {
		assert.Len(t, data, 1)
		assert.Equal(t, "testEntityDirtyQueueAge", data[0].TableSchema.GetTableName())
		assert.Equal(t, uint64(1), data[0].ID)
		assert.False(t, data[0].Added)
		assert.True(t, data[0].Updated)
		assert.False(t, data[0].Deleted)
		return nil
	})
	assert.Nil(t, err)

	engine.MarkToDelete(entityAge)
	err = engine.Flush()
	assert.Nil(t, err)

	err = receiver.Digest(queueName, func(data []*DirtyData) error {
		assert.Len(t, data, 1)
		assert.Equal(t, "testEntityDirtyQueueAge", data[0].TableSchema.GetTableName())
		assert.Equal(t, uint64(1), data[0].ID)
		assert.False(t, data[0].Added)
		assert.False(t, data[0].Updated)
		assert.True(t, data[0].Deleted)
		return nil
	})
	assert.Nil(t, err)
}
