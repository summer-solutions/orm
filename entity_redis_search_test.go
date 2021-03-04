package orm

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type redisSearchEntity struct {
	ORM  `orm:"redisSearch=search"`
	ID   uint
	Name string `orm:"searchable"`
	Age  uint64 `orm:"searchable;sortable"`
}

func TestEntityRedisSearch(t *testing.T) {
	var entity *redisSearchEntity
	engine := PrepareTables(t, &Registry{}, 5, entity)

	indexer := NewRedisSearchIndexer(engine)
	indexer.DisableLoop()
	indexer.Run(context.Background())

	flusher := engine.NewFlusher()
	for i := 1; i <= 5; i++ {
		flusher.Track(&redisSearchEntity{Name: "Name " + strconv.Itoa(i), Age: uint64(i)})
	}
	flusher.Flush()

	indices := engine.GetRedisSearch("search").ListIndices()
	assert.Len(t, indices, 1)
	assert.True(t, strings.HasPrefix(indices[0], "orm.redisSearchEntity:"))
	info := engine.GetRedisSearch("search").Info(indices[0])
	assert.False(t, info.Indexing)
	assert.True(t, info.Options.NoFreqs)
	assert.False(t, info.Options.NoFields)
	assert.True(t, info.Options.NoOffsets)
	assert.False(t, info.Options.MaxTextFields)
	assert.Equal(t, []string{"613b9:"}, info.Definition.Prefixes)
	assert.Len(t, info.Fields, 1)
	assert.Equal(t, "Age", info.Fields[0].Name)
	assert.Equal(t, "NUMERIC", info.Fields[0].Type)
	assert.True(t, info.Fields[0].Sortable)
	assert.False(t, info.Fields[0].NoIndex)

	// TODO check info.Fields[0]

	//ids, total := engine.RedisSearchIds(entity, NewPager(1, 10))
	//assert.Equal(t, uint64(5), total)
	//assert.Len(t, ids, 5)
}
