package orm

import "reflect"

func clearByIds(engine *Engine, entity interface{}, ids ...uint64) error {
	entityType := reflect.ValueOf(entity).Elem().Type()
	schema := getTableSchema(engine.config, entityType)
	cacheKeys := make([]string, len(ids))
	for i, id := range ids {
		cacheKeys[i] = schema.getCacheKey(id)
	}
	localCache := schema.GetLocalCache(engine)
	if localCache != nil {
		localCache.Remove(cacheKeys...)
	}
	redisCache := schema.GetRedisCacheContainer(engine)
	var err error
	if redisCache != nil {
		err = redisCache.Del(cacheKeys...)
	}
	return err
}
