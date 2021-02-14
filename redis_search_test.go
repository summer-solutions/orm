package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRedisSearch(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6383", 0)
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	engine := validatedRegistry.CreateEngine()
	engine.GetRedis().FlushDB()
	search := engine.GetRedisSearch()
	assert.NotNil(t, search)
	alters := engine.GetRedisSearchIndexAlters()
	assert.Nil(t, alters)
}
