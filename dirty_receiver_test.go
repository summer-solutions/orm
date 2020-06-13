package orm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type dirtyReceiverEntity struct {
	ORM  `orm:"dirty=entity_changed"`
	ID   uint
	Name string `orm:"dirty=name_changed"`
	Age  uint64
}

func TestDirtyReceiver(t *testing.T) {
	var entity *dirtyReceiverEntity
	registry := &Registry{}
	registry.RegisterDirtyQueue("entity_changed", 2)
	registry.RegisterDirtyQueue("name_changed", 1)
	engine := PrepareTables(t, registry, entity)

	receiver := NewDirtyReceiver(engine)
	receiver.DisableLoop()
	receiver.SetMaxLoopDuration(time.Millisecond)
	receiver.Purge("entity_changed")
	receiver.Purge("name_changed")

	e := &dirtyReceiverEntity{Name: "John", Age: 18}
	engine.Track(e)
	e = &dirtyReceiverEntity{Name: "Tom", Age: 18}
	engine.Track(e)
	engine.Flush()

	valid := false
	validHeartBeat := false
	receiver.SetHeartBeat(func() {
		validHeartBeat = true
	})
	receiver.Digest("entity_changed", func(data []*DirtyData) {
		valid = true
		assert.Len(t, data, 2)
		assert.Equal(t, uint64(1), data[0].ID)
		assert.Equal(t, uint64(2), data[1].ID)
		assert.True(t, data[0].Added)
		assert.True(t, data[1].Added)
		assert.False(t, data[0].Updated)
		assert.False(t, data[1].Updated)
		assert.False(t, data[0].Deleted)
		assert.False(t, data[1].Deleted)
		assert.Equal(t, "dirtyReceiverEntity", data[0].TableSchema.GetTableName())
		assert.Equal(t, "dirtyReceiverEntity", data[1].TableSchema.GetTableName())
	})
	assert.True(t, valid)
	assert.True(t, validHeartBeat)

	valid = false
	receiver.Digest("name_changed", func(data []*DirtyData) {
		valid = true
		assert.Len(t, data, 1)
		assert.Equal(t, uint64(1), data[0].ID)
		assert.True(t, data[0].Added)
		assert.False(t, data[0].Updated)
		assert.False(t, data[0].Deleted)
		assert.Equal(t, "dirtyReceiverEntity", data[0].TableSchema.GetTableName())
	})
	assert.True(t, valid)
	valid = false
	receiver.Digest("name_changed", func(data []*DirtyData) {
		valid = true
		assert.Len(t, data, 1)
		assert.Equal(t, uint64(2), data[0].ID)
	})
	assert.True(t, valid)

	e.Name = "Bob"
	engine.TrackAndFlush(e)
	valid = false
	receiver.Digest("entity_changed", func(data []*DirtyData) {
		valid = true
		assert.Len(t, data, 1)
		assert.Equal(t, uint64(2), data[0].ID)
		assert.False(t, data[0].Added)
		assert.True(t, data[0].Updated)
		assert.False(t, data[0].Deleted)
		assert.Equal(t, "dirtyReceiverEntity", data[0].TableSchema.GetTableName())
	})
	assert.True(t, valid)
	valid = false
	receiver.Digest("name_changed", func(data []*DirtyData) {
		valid = true
		assert.Len(t, data, 1)
		assert.Equal(t, uint64(2), data[0].ID)
		assert.False(t, data[0].Added)
		assert.True(t, data[0].Updated)
		assert.False(t, data[0].Deleted)
		assert.Equal(t, "dirtyReceiverEntity", data[0].TableSchema.GetTableName())
	})
	assert.True(t, valid)

	e.Age = 30
	engine.TrackAndFlush(e)
	valid = false
	receiver.Digest("entity_changed", func(data []*DirtyData) {
		valid = true
		assert.Len(t, data, 1)
		assert.Equal(t, uint64(2), data[0].ID)
	})
	assert.True(t, valid)
	valid = true
	receiver.Digest("name_changed", func(data []*DirtyData) {
		valid = false
	})
	assert.True(t, valid)

	engine.MarkToDelete(e)
	engine.Flush()

	valid = false
	receiver.Digest("entity_changed", func(data []*DirtyData) {
		valid = true
		assert.Len(t, data, 1)
		assert.Equal(t, uint64(2), data[0].ID)
		assert.False(t, data[0].Added)
		assert.False(t, data[0].Updated)
		assert.True(t, data[0].Deleted)
		assert.Equal(t, "dirtyReceiverEntity", data[0].TableSchema.GetTableName())
	})
	assert.True(t, valid)

	valid = false
	receiver.Digest("name_changed", func(data []*DirtyData) {
		valid = true
		assert.Len(t, data, 1)
		assert.Equal(t, uint64(2), data[0].ID)
		assert.False(t, data[0].Added)
		assert.False(t, data[0].Updated)
		assert.True(t, data[0].Deleted)
		assert.Equal(t, "dirtyReceiverEntity", data[0].TableSchema.GetTableName())
	})
	assert.True(t, valid)
}
