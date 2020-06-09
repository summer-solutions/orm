package orm

func clearByIDs(engine *Engine, entity Entity, ids ...uint64) {
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
		redisCache.Del(cacheKeys...)
	}
}
