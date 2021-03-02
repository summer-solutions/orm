package tools

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

func TestRedisSearchStatistics(t *testing.T) {
	registry := &orm.Registry{}
	registry.RegisterRedis("localhost:6383", 0)
	registry.RegisterRedis("localhost:6383", 1, "another")
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	engine := validatedRegistry.CreateEngine()
	r := engine.GetRedis()
	r.FlushDB()

	stats := GetRedisSearchStatistics(engine)
	assert.Nil(t, stats)
}
