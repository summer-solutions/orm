package orm

import (
	"strconv"
)

func flushInCache(engine *Engine) {
	invalidEntities := make([]Entity, 0)
	validEntities := make([][]byte, 0)
	redisValues := make(map[string][]interface{})
	localValues := make(map[string][]interface{})

	for _, entity := range engine.trackedEntities {
		orm := initIfNeeded(engine, entity)

		id := entity.GetID()
		entityName := orm.tableSchema.t.String()
		schema := orm.tableSchema
		cacheLocal, hasLocal := schema.GetLocalCache(engine)
		cacheRedis, hasRedis := schema.GetRedisCache(engine)
		if (!hasLocal && !hasRedis) || id == 0 {
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
			if hasRedis {
				entityCacheRedisValue := buildRedisValue(entity)
				if redisValues[cacheRedis.code] == nil {
					redisValues[cacheRedis.code] = make([]interface{}, 0)
				}
				redisValues[cacheRedis.code] = append(redisValues[cacheRedis.code], entityCacheKey, entityCacheRedisValue)
			}
			if hasLocal {
				entityCacheLocalValue := buildLocalCacheValue(entity)
				if localValues[cacheLocal.code] == nil {
					localValues[cacheLocal.code] = make([]interface{}, 0)
				}
				localValues[cacheLocal.code] = append(localValues[cacheLocal.code], entityCacheKey, entityCacheLocalValue)
			}
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
		for cacheCode, keys := range localValues {
			engine.GetLocalCache(cacheCode).MSet(keys...)
		}
	}
	engine.trackedEntities = make([]Entity, 0)
}

func createDirtyQueueMember(entityName string, id uint64) []byte {
	return []byte(entityName + ":" + strconv.FormatUint(id, 10))
}
