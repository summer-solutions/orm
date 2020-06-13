package orm

import (
	"testing"

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

	receiverAll := NewDirtyReceiver(engine)
	receiverAll.DisableLoop()
	receiverName := NewDirtyReceiver(engine)
	receiverName.DisableLoop()

	e := &dirtyReceiverEntity{Name: "John", Age: 18}
	engine.Track(e)
	e = &dirtyReceiverEntity{Name: "Tom", Age: 18}
	engine.Track(e)
	engine.Flush()

	valid := false
	validHeartBeat := false
	receiverAll.SetHeartBeat(func() {
		validHeartBeat = true
	})
	receiverAll.Digest("entity_changed", func(data []*DirtyData) {
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
}
