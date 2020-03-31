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
		return EntityNotRegistered{Name: entityType.String()}
	}
	cacheKeys := make([]string, len(ids))
	for i, id := range ids {
		cacheKeys[i] = schema.getCacheKey(id)
	}
	localCache := schema.GetLocalCache(engine)
	if localCache != nil {
		localCache.Remove(cacheKeys...)
	}
	redisCache := schema.GetRedisCacheContainer(engine)
	if redisCache != nil {
		return redisCache.Del(cacheKeys...)
	}
	return nil
}
