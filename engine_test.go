package orm

import (
	"testing"

	"github.com/stretchr/testify/require"

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
	registry.RegisterMySQLPool("root:root@tcp(localhost:3310)/test")
	registry.RegisterRabbitMQServer("amqp://rabbitmq_user:rabbitmq_password@localhost:5677/test")
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()
	assert.NotNil(t, engine)

	require.PanicsWithError(t, "track limit 10000 exceeded", func() {
		for i := 0; i < 10001; i++ {
			engine.Track(&testEntityEngine{})
		}
	})

	engine.ClearTrackedEntities()
	assert.Len(t, engine.trackedEntities, 0)

	require.PanicsWithError(t, "unregistered mysql pool 'test'", func() {
		engine.GetMysql("test")
	})
	require.PanicsWithError(t, "unregistered local cache pool 'test'", func() {
		engine.GetLocalCache("test")
	})
	require.PanicsWithError(t, "unregistered redis cache pool 'test'", func() {
		engine.GetRedis("test")
	})
	require.PanicsWithError(t, "unregistered locker pool 'test'", func() {
		engine.GetLocker("test")
	})
	require.PanicsWithError(t, "entity 'orm.testEntityEngineUnregistered' is not registered", func() {
		engine.Track(&testEntityEngineUnregistered{})
	})
}
