package tools

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

func TestRedisSearchStatistics(t *testing.T) {
	registry := &orm.Registry{}
	registry.RegisterRedis("localhost:6383", 0)
	registry.RegisterRedisSearchIndex(&orm.RedisSearchIndex{Name: "test", RedisPool: "default"})
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	engine := validatedRegistry.CreateEngine()
	engine.GetRedis().FlushDB()
	for _, alter := range engine.GetRedisSearchIndexAlters() {
		alter.Execute()
	}
	stats := GetRedisSearchStatistics(engine)
	assert.Len(t, stats, 1)
	assert.Equal(t, "test", stats[0].Index.Name)
	assert.Len(t, stats[0].Versions, 0)
	assert.True(t, stats[0].ForceReindex)
	assert.Equal(t, uint64(0), stats[0].ForceReindexLastID)
	assert.Greater(t, stats[0].ForceReindexVersion, uint64(0))

	indexer := orm.NewRedisSearchIndexer(engine)
	indexer.DisableLoop()
	indexer.Run(context.Background())

	stats = GetRedisSearchStatistics(engine)
	assert.Len(t, stats, 1)
	assert.Equal(t, "test", stats[0].Index.Name)
	assert.Len(t, stats[0].Versions, 1)
	assert.True(t, stats[0].Versions[0].Current)
	assert.False(t, stats[0].ForceReindex)
	assert.Equal(t, uint64(0), stats[0].ForceReindexLastID)
	assert.Equal(t, uint64(0), stats[0].ForceReindexVersion)
}
