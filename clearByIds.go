package orm

import (
	"reflect"
)

func clearByIds(engine *Engine, entity interface{}, ids ...uint64) error {
	entityType := reflect.ValueOf(entity).Elem().Type()
	schema, has, err := getTableSchema(engine.config, entityType)
	if err != nil {
		return err
	}
	if !has {
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
	redisCache, has := schema.GetRedisCacheContainer(engine)
	if has {
		return redisCache.Del(cacheKeys...)
	}
	return nil
}
