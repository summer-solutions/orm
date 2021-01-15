package orm

import (
	"context"
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
	registry.RegisterRedisStream("entity_changed", "default", []string{"test-group-1"})
	registry.RegisterRedisStream("name_changed", "default", []string{"test-group-2"})
	engine := PrepareTables(t, registry, 5, entity)
	ctx := context.Background()

	channels := engine.GetRegistry().GetRedisStreams()
	assert.Len(t, channels, 1)
	assert.Len(t, channels["default"], 4)

	consumer := NewDirtyConsumer(engine, "default-consumer", "test-group-1", 1)
	consumer.DisableLoop()
	consumer.block = time.Millisecond
	consumer2 := NewDirtyConsumer(engine, "default-consumer", "test-group-2", 1)
	consumer2.DisableLoop()
	consumer2.block = time.Millisecond

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
	consumer.Digest(ctx, 2, func(data []*DirtyData) {
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
	consumer2.Digest(ctx, 1, func(data []*DirtyData) {
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
	consumer.Digest(ctx, 2, func(data []*DirtyData) {
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
	consumer2.Digest(ctx, 1, func(data []*DirtyData) {
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
	consumer.Digest(ctx, 2, func(data []*DirtyData) {
		valid = true
		assert.Len(t, data, 1)
		assert.Equal(t, uint64(2), data[0].ID)
	})
	assert.True(t, valid)
	valid = true
	consumer.Digest(ctx, 1, func(data []*DirtyData) {
		valid = false
	})
	assert.True(t, valid)

	e.Name = "test transaction"
	engine.Track(e)
	engine.FlushInTransaction()
	valid = false
	consumer.Digest(ctx, 2, func(data []*DirtyData) {
		valid = true
		assert.Len(t, data, 1)
		assert.Equal(t, uint64(2), data[0].ID)
	})
	assert.True(t, valid)

	valid = false
	consumer2.Digest(ctx, 1, func(data []*DirtyData) {
		valid = true
	})
	assert.True(t, valid)

	engine.MarkToDelete(e)
	engine.Flush()

	valid = false
	consumer.Digest(ctx, 2, func(data []*DirtyData) {
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
	consumer2.Digest(ctx, 1, func(data []*DirtyData) {
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
	consumer2.Digest(ctx, 1, func(data []*DirtyData) {
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
