package orm

import (
	"reflect"
)

func clearByIDs(engine *Engine, entity interface{}, ids ...uint64) error {
	entityType := reflect.ValueOf(entity).Elem().Type()
	schema := getTableSchema(engine.registry, entityType)
	if schema == nil {
		return EntityNotRegisteredError{Name: entityType.String()}
	}
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
		return redisCache.Del(cacheKeys...)
	}
	return nil
}
