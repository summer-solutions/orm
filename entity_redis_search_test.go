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
	for i := 1; i <= 50; i++ {
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

	query := &RedisSearchQuery{}
	query.Sort("Age", false)
	ids, total := engine.RedisSearchIds(entity, query, NewPager(1, 10))
	assert.Equal(t, uint64(50), total)
	assert.Equal(t, uint64(1), ids[0])
	assert.Equal(t, uint64(2), ids[1])
	assert.Equal(t, uint64(10), ids[9])
	assert.Len(t, ids, 10)
	query.FilterIntMinMax("Age", 6, 8)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 10))
	assert.Equal(t, uint64(3), total)
	assert.Len(t, ids, 3)
	assert.Equal(t, uint64(6), ids[0])
	assert.Equal(t, uint64(7), ids[1])
	assert.Equal(t, uint64(8), ids[2])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterIntGreaterEqual("Age", 20)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 10))
	assert.Equal(t, uint64(31), total)
	assert.Len(t, ids, 10)
	assert.Equal(t, uint64(20), ids[0])
	assert.Equal(t, uint64(21), ids[1])
	assert.Equal(t, uint64(29), ids[9])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterIntLessEqual("Age", 20)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 30))
	assert.Equal(t, uint64(20), total)
	assert.Len(t, ids, 20)
	assert.Equal(t, uint64(20), ids[19])
	assert.Equal(t, uint64(19), ids[18])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterIntGreater("Age", 20)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 10))
	assert.Equal(t, uint64(30), total)
	assert.Len(t, ids, 10)
	assert.Equal(t, uint64(21), ids[0])
	assert.Equal(t, uint64(22), ids[1])
	assert.Equal(t, uint64(30), ids[9])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterIntLess("Age", 20)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 30))
	assert.Equal(t, uint64(19), total)
	assert.Len(t, ids, 19)
	assert.Equal(t, uint64(19), ids[18])
	assert.Equal(t, uint64(18), ids[17])

	//query = &RedisSearchQuery{}
	//query.Sort("Age", false)
	//query.FilterIntIn("Age", 18, 38)
	//ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 30))
	//assert.Equal(t, uint64(2), total)
	//assert.Len(t, ids, 2)
	//assert.Equal(t, uint64(18), ids[0])
	//assert.Equal(t, uint64(38), ids[1])
}
