package orm

import (
	"strconv"
)

func flushInCache(engine *Engine, entities ...Entity) {
	invalidEntities := make([]Entity, 0)
	validEntities := make([][]byte, 0)
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
		flush(engine, false, false, invalidEntities...)
	}
	if len(validEntities) > 0 {
		channel := engine.GetRabbitMQQueue(flushCacheQueueName)
		for _, v := range validEntities {
			channel.Publish(v)
		}
		for cacheCode, keys := range redisValues {
			engine.GetRedis(cacheCode).MSet(keys...)
		}
	}
}

func createDirtyQueueMember(entityName string, id uint64) []byte {
	return []byte(entityName + ":" + strconv.FormatUint(id, 10))
}
