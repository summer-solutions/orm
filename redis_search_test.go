package orm

import (
	"testing"

	apexLog "github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

func TestRedisSearch(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6383", 0)
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	engine := validatedRegistry.CreateEngine()
	engine.GetRedis().FlushDB()

	testLog := memory.New()
	engine.AddQueryLogger(testLog, apexLog.InfoLevel, QueryLoggerSourceRedis)

	search := engine.GetRedisSearch()
	assert.NotNil(t, search)
	alters := engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters, 0)

	testIndex := RedisSearchIndex{Name: "test", RedisPool: "default"}
	search.createIndex(testIndex)
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters, 1)
	assert.Equal(t, "default", alters[0].Pool)
	assert.Equal(t, "FT.DROPINDEX test DD", alters[0].Query)
	assert.True(t, alters[0].Safe)
	alters[0].Execute()
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters, 0)
}
