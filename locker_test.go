package orm

import (
	"testing"
	"time"

	apexLog "github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

func TestLocker(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6381", 15)
	registry.RegisterLocker("default", "default")
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()
	engine.GetRedis().FlushDB()
	testLogger := memory.New()
	engine.AddQueryLogger(testLogger, apexLog.InfoLevel, QueryLoggerSourceRedis)

	l := engine.GetLocker()
	lock, has := l.Obtain(engine.context, "test_key", time.Second, 0)
	assert.True(t, has)
	assert.NotNil(t, lock)

	_, has = l.Obtain(engine.context, "test_key", time.Second, time.Millisecond)
	assert.False(t, has)

	left := lock.TTL()
	assert.LessOrEqual(t, left.Microseconds(), time.Second.Microseconds())

	lock.Release()
	lock.Release()
	_, has = l.Obtain(engine.context, "test_key", time.Second, time.Millisecond)
	assert.True(t, has)

	assert.PanicsWithError(t, "ttl must be higher than zero", func() {
		_, _ = l.Obtain(engine.context, "test_key", 0, time.Millisecond)
	})

	registry = &Registry{}
	registry.RegisterRedis("localhost:6389", 15)
	registry.RegisterLocker("default", "default")
	validatedRegistry, err = registry.Validate()
	assert.NoError(t, err)
	engine = validatedRegistry.CreateEngine()
	testLogger = memory.New()
	engine.AddQueryLogger(testLogger, apexLog.InfoLevel, QueryLoggerSourceRedis)
	l = engine.GetLocker()
	assert.Panics(t, func() {
		_, _ = l.Obtain(engine.context, "test_key", time.Second, time.Millisecond)
	})
}
