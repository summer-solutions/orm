package orm

import "github.com/juju/errors"

func clearByIDs(engine *Engine, entity Entity, ids ...uint64) error {
	schema := initIfNeeded(engine, entity).tableSchema
	cacheKeys := make([]string, len(ids))
	for i, id := range ids {
		cacheKeys[i] = schema.getCacheKey(id)
	}
	localCache, has := schema.GetLocalCache(engine)
	if has {
		localCache.Remove(cacheKeys...)
	}
	redisCache, has := schema.GetRedisCache(engine)
	if has {
		err := redisCache.Del(cacheKeys...)
		if err != nil {
			return errors.Trace(err)
		}
	}
	return nil
}
