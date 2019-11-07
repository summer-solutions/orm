package orm

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"reflect"
	"time"
)

func FlushInCache(entities ...interface{}) error {

	invalidEntities := make([]interface{}, 0)
	validEntities := make([]*redis.Z, 0)
	redisValues := make(map[string][]interface{})

	for _, entity := range entities {

		v := reflect.ValueOf(entity)
		value := reflect.Indirect(v)
		t := value.Type()

		id := value.Field(1).Uint()
		entityName := t.String()
		schema := GetTableSchema(entityName)
		cache := schema.GetRedisCacheContainer()
		if cache == nil || id == 0 {
			invalidEntities = append(invalidEntities, entity)
		} else {

			isDirty, bind := IsDirty(entity)
			if !isDirty {
				continue
			}
			orm := value.Field(0).Interface().(ORM)
			old := make(map[string]interface{}, len(orm.DBData))
			for k, v := range orm.DBData {
				old[k] = v
			}
			injectBind(value, bind)
			entityCacheKey := schema.getCacheKey(id)
			entityCacheValue := buildRedisValue(entity, schema)
			if redisValues[cache.code] == nil {
				redisValues[cache.code] = make([]interface{}, 0)
			}
			redisValues[cache.code] = append(redisValues[cache.code], entityCacheKey, entityCacheValue)

			validEntities = append(validEntities, createDirtyQueueMember(entityName, id))
		}
	}
	if len(invalidEntities) > 0 {
		err := Flush(invalidEntities...)
		if err != nil {
			return err
		}
	}
	if len(validEntities) > 0 {
		GetRedisCache(queueRedisName).ZAdd("dirty_queue", validEntities...)
		for cacheCode, keys := range redisValues {
			GetRedisCache(cacheCode).MSet(keys...)
		}
	}
	return nil
}

func createDirtyQueueMember(entityName string, id uint64) *redis.Z {
	return &redis.Z{Score: float64(time.Now().Unix()), Member: fmt.Sprintf("%s:%d", entityName, id)}
}
