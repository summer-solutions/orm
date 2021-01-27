package tools

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

func TestRedisStatistics(t *testing.T) {
	registry := &orm.Registry{}
	registry.RegisterRedis("localhost:6381", 15)
	registry.RegisterRedis("localhost:6381", 14, "another")
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	engine := validatedRegistry.CreateEngine()
	r := engine.GetRedis()
	r.FlushDB()

	stats := GetRedisStatistics(engine)
	assert.Len(t, stats, 1)
	assert.Equal(t, "default", stats[0].RedisPool)
}
