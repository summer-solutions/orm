package orm

import (
	"strconv"
	"testing"

	"github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

type testEntityFlushLazyLocal struct {
	ORM  `orm:"localCache"`
	ID   uint
	Name string
}

func TestFlushLazyLocal(t *testing.T) {
	var entity testEntityFlushLazyLocal
	engine := PrepareTables(t, &Registry{}, entity)
	defer engine.Defer()

	DBLogger := memory.New()
	pool := engine.GetMysql()
	pool.AddLogger(DBLogger)
	pool.SetLogLevel(log.InfoLevel)

	var entities = make([]interface{}, 10)
	for i := 1; i <= 10; i++ {
		e := &testEntityFlushLazyLocal{Name: "Name " + strconv.Itoa(i)}
		entities[i-1] = e
		engine.Track(e)
	}
	err := engine.FlushLazy()
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Entries, 0)

	receiver := NewLazyReceiver(engine)
	receiver.DisableLoop()
	err = receiver.Digest()
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Entries, 1)
	found, err := engine.LoadByID(1, &entity)
	assert.Nil(t, err)
	assert.True(t, found)
	assert.Equal(t, "Name 1", entity.Name)

	DBLogger.Entries = make([]*log.Entry, 0)
	engine.Track(&entity)
	entity.Name = "Name 1.1"
	err = engine.FlushLazy()
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Entries, 0)

	err = receiver.Digest()
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Entries, 1)
	found, err = engine.LoadByID(1, &entity)
	assert.Nil(t, err)
	assert.True(t, found)
	assert.Equal(t, "Name 1.1", entity.Name)

	DBLogger.Entries = make([]*log.Entry, 0)
	engine.MarkToDelete(&entity)
	err = engine.FlushLazy()
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Entries, 0)

	err = receiver.Digest()
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Entries, 1)
	found, err = engine.LoadByID(1, &entity)
	assert.Nil(t, err)
	assert.False(t, found)
}
