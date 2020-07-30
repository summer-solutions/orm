package orm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type lazyReceiverEntity struct {
	ORM          `orm:"localCache;redisCache"`
	ID           uint
	Name         string `orm:"unique=name"`
	Age          uint64
	EnumNullable string       `orm:"enum=orm.TestEnum"`
	IndexAll     *CachedQuery `query:""`
}

func TestLazyReceiver(t *testing.T) {
	var entity *lazyReceiverEntity

	registry := &Registry{}
	registry.RegisterEnumMap("orm.TestEnum", map[string]string{"a": "a", "b": "b", "c": "c"}, "a")
	engine := PrepareTables(t, registry, entity)

	receiver := NewLazyReceiver(engine)
	receiver.DisableLoop()
	receiver.SetMaxLoopDuration(time.Millisecond)
	receiver.Purge()

	e := &lazyReceiverEntity{Name: "John", Age: 18}
	engine.Track(e)
	engine.FlushLazy()

	e = &lazyReceiverEntity{}
	loaded := engine.LoadByID(1, e)
	assert.False(t, loaded)

	validHeartBeat := false
	receiver.SetHeartBeat(func() {
		validHeartBeat = true
	})
	receiver.Digest()
	assert.True(t, validHeartBeat)

	loaded = engine.LoadByID(1, e)
	assert.True(t, loaded)
	assert.Equal(t, "John", e.Name)
	assert.Equal(t, uint64(18), e.Age)

	e.Name = "Tom"
	engine.Track(e)
	engine.FlushLazy()

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
	receiver.SetHeartBeat(func() {
		validHeartBeat = true
	})
	receiver.Digest()
	assert.True(t, validHeartBeat)

	e = &lazyReceiverEntity{}
	loaded = engine.LoadByID(1, e)
	assert.True(t, loaded)
	assert.Equal(t, "Tom", e.Name)

	e = &lazyReceiverEntity{Name: "Tom"}
	engine.Track(e)
	engine.FlushLazy()

	validHeartBeat = false
	receiver.SetHeartBeat(func() {
		validHeartBeat = true
	})
	receiver.Digest()
	assert.True(t, validHeartBeat)

	e = &lazyReceiverEntity{Name: "Adam", EnumNullable: "wrong"}
	engine.Track(e)
	engine.FlushLazy()

	assert.Panics(t, func() {
		receiver.Digest()
	})
}
