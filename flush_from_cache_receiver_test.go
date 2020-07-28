package orm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type flashCacheReceiverEntity struct {
	ORM          `orm:"localCache;redisCache"`
	ID           uint
	Name         string `orm:"unique=name"`
	Age          uint64
	EnumNullable string `orm:"enum=orm.TestEnum"`
}

func TestFlashFromCacheReceiver(t *testing.T) {
	var entity *flashCacheReceiverEntity

	registry := &Registry{}
	registry.RegisterEnumStruct("orm.TestEnum", TestEnum)
	engine := PrepareTables(t, registry, entity)

	receiver := NewFlushFromCacheReceiver(engine)
	receiver.DisableLoop()
	receiver.SetMaxLoopDuration(time.Millisecond)
	receiver.Purge()

	e := &flashCacheReceiverEntity{Name: "John", Age: 18}
	engine.Track(e)
	engine.FlushInCache()

	e = &flashCacheReceiverEntity{}
	loaded := engine.LoadByID(1, e)
	assert.True(t, loaded)

	validHeartBeat := false
	receiver.SetHeartBeat(func() {
		validHeartBeat = true
	})
	receiver.Digest()
	assert.False(t, validHeartBeat)

	e.Name = "Tom"
	engine.Track(e)
	engine.FlushInCache()
	e = &flashCacheReceiverEntity{}
	loaded = engine.LoadByID(1, e)
	assert.True(t, loaded)
	assert.Equal(t, "Tom", e.Name)
	engine.GetLocalCache().Clear()
	engine.GetRedis().FlushDB()

	e = &flashCacheReceiverEntity{}
	loaded = engine.LoadByID(1, e)
	assert.True(t, loaded)
	assert.Equal(t, "John", e.Name)
	e.Name = "Tom"
	engine.Track(e)
	engine.FlushInCache()

	validHeartBeat = false
	receiver.SetHeartBeat(func() {
		validHeartBeat = true
	})
	receiver.Digest()
	assert.True(t, validHeartBeat)

	engine.GetLocalCache().Clear()
	engine.GetRedis().FlushDB()
	e = &flashCacheReceiverEntity{}
	loaded = engine.LoadByID(1, e)
	assert.True(t, loaded)
	assert.Equal(t, "Tom", e.Name)

	engine.Track(e)
	engine.FlushInCache()
}
