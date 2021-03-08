package orm

import (
	"context"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

type redisSearchEntity struct {
	ORM             `orm:"redisSearch=search"`
	ID              uint
	Name            string  `orm:"searchable"`
	Age             uint64  `orm:"searchable;sortable"`
	Balance         int64   `orm:"sortable"`
	Weight          float64 `orm:"searchable"`
	AgeNullable     *uint64 `orm:"searchable"`
	BalanceNullable *int64  `orm:"searchable"`
	Enum            string  `orm:"enum=orm.TestEnum;required;searchable"`
}

func TestEntityRedisSearch(t *testing.T) {
	var entity *redisSearchEntity
	registry := &Registry{}
	registry.RegisterEnumStruct("orm.TestEnum", TestEnum)
	engine := PrepareTables(t, registry, 5, entity)

	indexer := NewRedisSearchIndexer(engine)
	indexer.DisableLoop()
	indexer.Run(context.Background())

	flusher := engine.NewFlusher()
	for i := 1; i <= 50; i++ {
		e := &redisSearchEntity{Name: "Name " + strconv.Itoa(i), Age: uint64(i)}
		e.Weight = 100.3 + float64(i)
		e.Balance = 20 - int64(i)
		e.Enum = TestEnum.A
		if i > 20 {
			v := uint64(i)
			e.AgeNullable = &v
			v2 := int64(i)
			e.BalanceNullable = &v2
			e.Enum = TestEnum.B
		}
		if i > 40 {
			e.Enum = TestEnum.C
		}
		flusher.Track(e)
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
	assert.Len(t, info.Fields, 6)
	assert.Equal(t, "Age", info.Fields[0].Name)
	assert.Equal(t, "NUMERIC", info.Fields[0].Type)
	assert.True(t, info.Fields[0].Sortable)
	assert.False(t, info.Fields[0].NoIndex)
	assert.Equal(t, "Balance", info.Fields[1].Name)
	assert.Equal(t, "NUMERIC", info.Fields[1].Type)
	assert.True(t, info.Fields[1].Sortable)
	assert.True(t, info.Fields[1].NoIndex)
	assert.Equal(t, "Weight", info.Fields[2].Name)
	assert.Equal(t, "NUMERIC", info.Fields[2].Type)
	assert.False(t, info.Fields[2].Sortable)
	assert.False(t, info.Fields[2].NoIndex)
	assert.Equal(t, "AgeNullable", info.Fields[3].Name)
	assert.Equal(t, "NUMERIC", info.Fields[3].Type)
	assert.False(t, info.Fields[3].Sortable)
	assert.False(t, info.Fields[3].NoIndex)
	assert.Equal(t, "BalanceNullable", info.Fields[4].Name)
	assert.Equal(t, "NUMERIC", info.Fields[4].Type)
	assert.False(t, info.Fields[4].Sortable)
	assert.False(t, info.Fields[4].NoIndex)
	assert.Equal(t, "Enum", info.Fields[5].Name)
	assert.Equal(t, "TAG", info.Fields[5].Type)
	assert.False(t, info.Fields[5].Sortable)
	assert.False(t, info.Fields[5].NoIndex)

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

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterInt("Age", 18)
	query.FilterInt("Age", 38)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 30))
	assert.Equal(t, uint64(2), total)
	assert.Len(t, ids, 2)
	assert.Equal(t, uint64(18), ids[0])
	assert.Equal(t, uint64(38), ids[1])

	query = &RedisSearchQuery{}
	query.Sort("Balance", false)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 3))
	assert.Equal(t, uint64(50), total)
	assert.Len(t, ids, 3)
	assert.Equal(t, uint64(50), ids[0])
	assert.Equal(t, uint64(49), ids[1])
	assert.Equal(t, uint64(48), ids[2])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterFloat("Weight", 101.3)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 10))
	assert.Equal(t, uint64(1), total)
	assert.Len(t, ids, 1)
	assert.Equal(t, uint64(1), ids[0])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterFloatMinMax("Weight", 105, 116.3)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 20))
	assert.Equal(t, uint64(12), total)
	assert.Len(t, ids, 12)
	assert.Equal(t, uint64(5), ids[0])
	assert.Equal(t, uint64(16), ids[11])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterFloatGreater("Weight", 148.3)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 20))
	assert.Equal(t, uint64(2), total)
	assert.Len(t, ids, 2)
	assert.Equal(t, uint64(49), ids[0])
	assert.Equal(t, uint64(50), ids[1])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterFloatGreaterEqual("Weight", 148.3)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 20))
	assert.Equal(t, uint64(3), total)
	assert.Len(t, ids, 3)
	assert.Equal(t, uint64(48), ids[0])
	assert.Equal(t, uint64(49), ids[1])
	assert.Equal(t, uint64(50), ids[2])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterFloatLess("Weight", 103.3)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 20))
	assert.Equal(t, uint64(2), total)
	assert.Len(t, ids, 2)
	assert.Equal(t, uint64(1), ids[0])
	assert.Equal(t, uint64(2), ids[1])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterFloatLessEqual("Weight", 103.3)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 20))
	assert.Equal(t, uint64(3), total)
	assert.Len(t, ids, 3)
	assert.Equal(t, uint64(1), ids[0])
	assert.Equal(t, uint64(2), ids[1])
	assert.Equal(t, uint64(3), ids[2])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterIntGreaterEqual("AgeNullable", 0)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 10))
	assert.Equal(t, uint64(30), total)
	assert.Len(t, ids, 10)
	assert.Equal(t, uint64(21), ids[0])
	assert.Equal(t, uint64(22), ids[1])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterIntNull("AgeNullable")
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 10))
	assert.Equal(t, uint64(20), total)
	assert.Len(t, ids, 10)
	assert.Equal(t, uint64(1), ids[0])
	assert.Equal(t, uint64(2), ids[1])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterIntGreaterEqual("BalanceNullable", 0)
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 10))
	assert.Equal(t, uint64(30), total)
	assert.Len(t, ids, 10)
	assert.Equal(t, uint64(21), ids[0])
	assert.Equal(t, uint64(22), ids[1])

	query = &RedisSearchQuery{}
	query.Sort("Age", false)
	query.FilterTag("Enum", "a", "c")
	ids, total = engine.RedisSearchIds(entity, query, NewPager(1, 50))
	assert.Equal(t, uint64(30), total)
	assert.Len(t, ids, 30)
	assert.Equal(t, uint64(1), ids[0])
	assert.Equal(t, uint64(41), ids[20])
	assert.Equal(t, uint64(50), ids[29])
}
