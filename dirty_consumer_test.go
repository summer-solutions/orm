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

func TestDirtyConsumer(t *testing.T) {
	var entity *dirtyReceiverEntity
	registry := &Registry{}
	engine := PrepareTables(t, registry, 5, entity)

	channels := engine.GetRegistry().GetRedisChannels()
	assert.Len(t, channels, 1)
	assert.Len(t, channels["default"], 4)
	assert.Equal(t, uint64(0), channels["default"]["dirty-channel-entity_changed"])
	assert.Equal(t, uint64(0), channels["default"]["dirty-channel-name_changed"])

	consumer := NewDirtyConsumer(engine)
	consumer.DisableLoop()
	consumer.block = time.Millisecond

	e := &dirtyReceiverEntity{Name: "John", Age: 18}
	engine.Track(e)
	e = &dirtyReceiverEntity{Name: "Tom", Age: 18}
	engine.Track(e)
	engine.Flush()

	valid := false
	validHeartBeat := false
	consumer.SetHeartBeat(time.Minute, func() {
		validHeartBeat = true
	})
	consumer.Digest([]string{"entity_changed"}, 2, func(data []*DirtyData) {
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

	iterations := 0
	consumer.Digest([]string{"name_changed"}, 1, func(data []*DirtyData) {
		iterations++
		assert.Len(t, data, 1)
		assert.Equal(t, uint64(iterations), data[0].ID)
		assert.True(t, data[0].Added)
		assert.False(t, data[0].Updated)
		assert.False(t, data[0].Deleted)
		assert.Equal(t, "dirtyReceiverEntity", data[0].TableSchema.GetTableName())
	})
	assert.Equal(t, 2, iterations)

	e.Name = "Bob"
	engine.TrackAndFlush(e)
	valid = false
	consumer.Digest([]string{"entity_changed"}, 2, func(data []*DirtyData) {
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
	consumer.Digest([]string{"name_changed"}, 1, func(data []*DirtyData) {
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
	consumer.Digest([]string{"entity_changed"}, 2, func(data []*DirtyData) {
		valid = true
		assert.Len(t, data, 1)
		assert.Equal(t, uint64(2), data[0].ID)
	})
	assert.True(t, valid)
	valid = true
	consumer.Digest([]string{"name_changed"}, 1, func(data []*DirtyData) {
		valid = false
	})
	assert.True(t, valid)

	e.Name = "test transaction"
	engine.Track(e)
	engine.FlushInTransaction()
	valid = false
	consumer.Digest([]string{"entity_changed"}, 2, func(data []*DirtyData) {
		valid = true
		assert.Len(t, data, 1)
		assert.Equal(t, uint64(2), data[0].ID)
	})
	assert.True(t, valid)

	valid = false
	consumer.Digest([]string{"name_changed"}, 1, func(data []*DirtyData) {
		valid = true
	})
	assert.True(t, valid)

	engine.MarkToDelete(e)
	engine.Flush()

	valid = false
	consumer.Digest([]string{"entity_changed"}, 2, func(data []*DirtyData) {
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
	consumer.Digest([]string{"name_changed"}, 1, func(data []*DirtyData) {
		valid = true
		assert.Len(t, data, 1)
		assert.Equal(t, uint64(2), data[0].ID)
		assert.False(t, data[0].Added)
		assert.False(t, data[0].Updated)
		assert.True(t, data[0].Deleted)
		assert.Equal(t, "dirtyReceiverEntity", data[0].TableSchema.GetTableName())
	})
	assert.True(t, valid)

	engine.MarkDirty(e, "name_changed", 2)
	valid = false
	consumer.Digest([]string{"name_changed"}, 1, func(data []*DirtyData) {
		valid = true
		assert.Len(t, data, 1)
		assert.Equal(t, uint64(2), data[0].ID)
		assert.False(t, data[0].Added)
		assert.True(t, data[0].Updated)
		assert.False(t, data[0].Deleted)
		assert.Equal(t, "dirtyReceiverEntity", data[0].TableSchema.GetTableName())
	})
	assert.True(t, valid)
}
