package orm

import (
	"testing"

	"github.com/apex/log"
	"github.com/stretchr/testify/assert"
)

type testEntityEngine struct {
	ORM
	ID uint
}

type testEntityEngineUnregistered struct {
	ORM
	ID uint
}

func TestEngine(t *testing.T) {
	registry := &Registry{}
	registry.RegisterEntity(testEntityEngine{})
	registry.RegisterMySQLPool("root:root@tcp(localhost:3308)/test")
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()
	assert.NotNil(t, engine)
	engine.EnableDebug()
	assert.NotNil(t, engine.log)
	assert.Equal(t, log.DebugLevel, engine.log.Level)
	engine.log = nil

	panicF = func(err error) {
		assert.EqualError(t, err, "track limit 10000 exceeded")
	}
	for i := 0; i < 10001; i++ {
		engine.Track(&testEntityEngine{})
	}
	engine.ClearTrackedEntities()
	assert.Len(t, engine.trackedEntities, 0)

	panicF = func(err error) {
		assert.EqualError(t, err, "unregistered mysql pool 'test'")
	}
	engine.GetMysql("test")
	panicF = func(err error) {
		assert.EqualError(t, err, "unregistered local cache pool 'test'")
	}
	engine.GetLocalCache("test")
	panicF = func(err error) {
		assert.EqualError(t, err, "unregistered redis cache pool 'test'")
	}
	engine.GetRedis("test")
	panicF = func(err error) {
		assert.EqualError(t, err, "unregistered locker pool 'test'")
	}
	engine.GetLocker("test")

	panicF = func(err error) {
		assert.EqualError(t, err, "entity 'orm.testEntityEngineUnregistered' is registered")
	}
	engine.Track(&testEntityEngineUnregistered{})
}
