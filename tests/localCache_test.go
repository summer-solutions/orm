package tests

import (
	"testing"

	"github.com/apex/log"

	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

func TestGetSetLocal(t *testing.T) {
	registry := &orm.Registry{}
	registry.RegisterLocalCache(10)
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()

	testLogger := memory.New()
	cache := engine.GetLocalCache()
	cache.AddLogger(testLogger)
	cache.SetLogLevel(log.InfoLevel)

	val := cache.GetSet("test", 1, func() interface{} {
		return "hello"
	})
	assert.Equal(t, "hello", val)
	assert.Len(t, testLogger.Entries, 2)
	assert.Equal(t, "[ORM][LOCAL][GET]", testLogger.Entries[0].Message)
	assert.Equal(t, "[ORM][LOCAL][MGET]", testLogger.Entries[1].Message)

	val = cache.GetSet("test", 1, func() interface{} {
		return "hello"
	})
	assert.Equal(t, "hello", val)
	assert.Len(t, testLogger.Entries, 3)
	assert.Equal(t, "[ORM][LOCAL][GET]", testLogger.Entries[2].Message)

	cache.HMset("test2", map[string]interface{}{"a": "b", "c": "d"})
	fields := cache.HMget("test2", "a", "c")
	assert.Len(t, fields, 2)
	assert.Equal(t, "b", fields["a"])
	assert.Equal(t, "d", fields["c"])
}
