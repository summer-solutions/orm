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

		value := reflect.Indirect(reflect.ValueOf(entity))
		orm := initIfNeeded(value, entity)
		value.Field(0).Interface().(*ORM).e = entity
		t := value.Type()

		id := value.Field(1).Uint()
		entityName := t.String()
		schema := getTableSchema(t)
		cache := schema.GetRedisCacheContainer()
		if cache == nil || id == 0 {
			invalidEntities = append(invalidEntities, entity)
		} else {
			isDirty, bind := orm.isDirty(value)
			if !isDirty {
				continue
			}
			old := make(map[string]interface{}, len(orm.dBData))
			for k, v := range orm.dBData {
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
