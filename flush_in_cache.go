package orm

import (
	"fmt"
)

func flushInCache(engine *Engine, entities ...Entity) error {
	invalidEntities := make([]Entity, 0)
	validEntities := make([]interface{}, 0)
	redisValues := make(map[string][]interface{})

	for _, entity := range entities {
		orm := initIfNeeded(engine, entity)

		id := entity.GetID()
		entityName := orm.tableSchema.t.String()
		schema := orm.tableSchema
		cache, hasRedis := schema.GetRedisCache(engine)
		if !hasRedis || id == 0 {
			invalidEntities = append(invalidEntities, entity)
		} else {
			isDirty, bind := getDirtyBind(entity.(Entity))
			if !isDirty {
				continue
			}
			old := make(map[string]interface{}, len(orm.dBData))
			for k, v := range orm.dBData {
				old[k] = v
			}
			injectBind(entity, bind)
			entityCacheKey := schema.getCacheKey(id)
			entityCacheValue := buildRedisValue(entity.(Entity))
			if redisValues[cache.code] == nil {
				redisValues[cache.code] = make([]interface{}, 0)
			}
			redisValues[cache.code] = append(redisValues[cache.code], entityCacheKey, entityCacheValue)

			validEntities = append(validEntities, createDirtyQueueMember(entityName, id))
		}
	}
	if len(invalidEntities) > 0 {
		err := flush(engine, false, false, invalidEntities...)
		if err != nil {
			return err
		}
	}
	if len(validEntities) > 0 {
		code := "default"
		redis := engine.getRedisForQueue(code)
		_, err := redis.SAdd("dirty_queue", validEntities...)
		if err != nil {
			return err
		}
		for cacheCode, keys := range redisValues {
			err = engine.GetRedis(cacheCode).MSet(keys...)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func createDirtyQueueMember(entityName string, id uint64) interface{} {
	return fmt.Sprintf("%s:%d", entityName, id)
}
