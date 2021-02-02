package orm

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type lazyReceiverEntity struct {
	ORM          `orm:"localCache;redisCache;asyncRedisLazyFlush=default"`
	ID           uint
	Name         string `orm:"unique=name"`
	Age          uint64
	EnumNullable string `orm:"enum=orm.TestEnum"`
	RefOne       *lazyReceiverReference
	IndexAll     *CachedQuery `query:""`
}

type lazyReceiverReference struct {
	ORM
	ID   uint
	Name string
}

func TestLazyReceiver(t *testing.T) {
	var entity *lazyReceiverEntity
	var ref *lazyReceiverReference

	registry := &Registry{}
	registry.RegisterEnumMap("orm.TestEnum", map[string]string{"a": "a", "b": "b", "c": "c"}, "a")
	engine := PrepareTables(t, registry, 5, entity, ref)
	engine.GetRedis().FlushDB()

	receiver := NewAsyncConsumer(engine, "default-consumer")
	receiver.DisableLoop()
	receiver.block = time.Millisecond

	e := &lazyReceiverEntity{Name: "John", Age: 18}
	engine.FlushLazy(e)

	e = &lazyReceiverEntity{}
	loaded := engine.LoadByID(1, e)
	assert.False(t, loaded)

	validHeartBeat := false
	receiver.SetHeartBeat(time.Minute, func() {
		validHeartBeat = true
	})
	receiver.Digest(context.Background(), 100)
	assert.True(t, validHeartBeat)

	loaded = engine.LoadByID(1, e)
	assert.True(t, loaded)
	assert.Equal(t, "John", e.Name)
	assert.Equal(t, uint64(18), e.Age)

	e.Name = "Tom"
	engine.FlushLazy(e)

	e = &lazyReceiverEntity{}
	loaded = engine.LoadByID(1, e)
	assert.True(t, loaded)
	assert.Equal(t, "Tom", e.Name)

	engine.GetLocalCache().Clear()
	e = &lazyReceiverEntity{}
	loaded = engine.LoadByID(1, e)
	assert.True(t, loaded)
	assert.Equal(t, "John", e.Name)

	validHeartBeat = false
	receiver.SetHeartBeat(time.Minute, func() {
		validHeartBeat = true
	})
	receiver.Digest(context.Background(), 100)
	assert.True(t, validHeartBeat)

	e = &lazyReceiverEntity{}
	loaded = engine.LoadByID(1, e)
	assert.True(t, loaded)
	assert.Equal(t, "Tom", e.Name)

	e = &lazyReceiverEntity{Name: "Tom"}
	engine.FlushLazy(e)

	validHeartBeat = false
	receiver.SetHeartBeat(time.Minute, func() {
		validHeartBeat = true
	})
	receiver.Digest(context.Background(), 100)
	assert.True(t, validHeartBeat)

	e = &lazyReceiverEntity{Name: "Adam", EnumNullable: "wrong"}
	engine.FlushLazy(e)

	assert.NotPanics(t, func() {
		receiver.Digest(context.Background(), 100)
	})
	e = &lazyReceiverEntity{Name: "Tom"}
	e.SetOnDuplicateKeyUpdate(map[string]interface{}{"Age": 38})
	assert.PanicsWithError(t, "lazy flush on duplicate key not supported", func() {
		engine.FlushLazy(e)
	})

	e = &lazyReceiverEntity{Name: "Adam", RefOne: &lazyReceiverReference{Name: "Test"}}
	assert.PanicsWithError(t, "lazy flush for unsaved references not supported", func() {
		engine.FlushLazy(e)
	})

	e = &lazyReceiverEntity{}
	engine.LoadByID(1, e)
	engine.NewFlusher().Delete(e).FlushLazy()
	receiver.Digest(context.Background(), 100)
	loaded = engine.LoadByID(1, e)
	assert.False(t, loaded)
}
