package orm

import (
	"strconv"
	"testing"

	"github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

type testEntityFlushLazyRedis struct {
	ORM  `orm:"redisCache"`
	ID   uint
	Name string
}

func TestFlushLazyRedis(t *testing.T) {
	var entity testEntityFlushLazyRedis
	engine := PrepareTables(t, &Registry{}, entity)

	DBLogger := memory.New()
	engine.AddLogger(DBLogger, log.InfoLevel, LoggerSourceDB)

	var entities = make([]interface{}, 10)
	for i := 1; i <= 10; i++ {
		e := &testEntityFlushLazyRedis{Name: "Name " + strconv.Itoa(i)}
		engine.Track(e)
		entities[i-1] = e
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
