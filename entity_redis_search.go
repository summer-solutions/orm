package orm

import (
	"strconv"

	"github.com/pkg/errors"
)

func (e *Engine) RedisSearchIds(entity Entity, pager *Pager, references ...string) (ids []uint64, totalRows uint64) {
	return redisSearch(e, entity, pager, references)
}

func redisSearch(e *Engine, entity Entity, pager *Pager, references []string) ([]uint64, uint64) {
	schema := e.GetRegistry().GetTableSchemaForEntity(entity).(*tableSchema)
	if schema.redisSearchIndex == nil {
		panic(errors.Errorf("entity %s is not searchable", schema.t.String()))
	}
	search := e.GetRedisSearch(schema.searchCacheName)
	query := &RedisSearchQuery{}
	totalRows, res := search.search(schema.redisSearchIndex.Name, query, pager, true)
	ids := make([]uint64, len(res))
	for i, v := range res {
		ids[i], _ = strconv.ParseUint(v.(string)[6:], 10, 64)
	}
	return ids, totalRows
}
