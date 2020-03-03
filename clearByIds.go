package orm

import "reflect"

func ClearByIds(entity interface{}, ids ...uint64) error {
	entityType := reflect.ValueOf(entity).Elem().Type()
	schema := getTableSchema(entityType)
	cacheKeys := make([]string, len(ids))
	for i, id := range ids {
		cacheKeys[i] = schema.getCacheKey(id)
	}
	localCache := schema.GetLocalCache()
	if localCache != nil {
		localCache.Remove(cacheKeys...)
	}
	redisCache := schema.GetRedisCacheContainer()
	var err error
	if redisCache != nil {
		err = redisCache.Del(cacheKeys...)
	}
	return err
}
