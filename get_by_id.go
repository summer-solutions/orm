package orm

import (
	"fmt"

	jsoniter "github.com/json-iterator/go"
)

func loadByID(engine *Engine, id uint64, entity Entity, useCache bool, references ...string) (found bool, err error) {
	orm := initIfNeeded(engine, entity)
	schema := orm.tableSchema
	var cacheKey string
	localCache, hasLocalCache := schema.GetLocalCache(engine)

	if hasLocalCache && useCache {
		cacheKey = schema.getCacheKey(id)
		e, has := localCache.Get(cacheKey)
		if has {
			if e == "nil" {
				return false, nil
			}
			fillFromDBRow(id, engine, e.([]string), entity)
			if len(references) > 0 {
				err = warmUpReferences(engine, schema, orm.attributes.elem, references, false)
			}
			return true, err
		}
	}
	redisCache, hasRedis := schema.GetRedisCache(engine)
	if hasRedis && useCache {
		cacheKey = schema.getCacheKey(id)
		row, has, err := redisCache.Get(cacheKey)
		if err != nil {
			return false, err
		}
		if has {
			if row == "nil" {
				return false, nil
			}
			var decoded []string
			err = jsoniter.ConfigFastest.Unmarshal([]byte(row), &decoded)
			if err != nil {
				return true, err
			}
			fillFromDBRow(id, engine, decoded, entity)
			if len(references) > 0 {
				err = warmUpReferences(engine, schema, orm.attributes.elem, references, false)
			}
			return true, err
		}
	}
	found, err = searchRow(false, engine, NewWhere("`ID` = ?", id), entity, nil)
	if err != nil {
		return false, err
	}
	if !found {
		if localCache != nil {
			localCache.Set(cacheKey, "nil")
		}
		if redisCache != nil {
			err = redisCache.Set(cacheKey, "nil", 60)
			if err != nil {
				return false, err
			}
		}
		return false, nil
	}
	if localCache != nil && useCache {
		localCache.Set(cacheKey, buildLocalCacheValue(entity))
	}
	if redisCache != nil && useCache {
		err = redisCache.Set(cacheKey, buildRedisValue(entity), 0)
		if err != nil {
			return false, err
		}
	}
	if len(references) > 0 {
		err = warmUpReferences(engine, schema, orm.attributes.elem, references, false)
		if err != nil {
			return true, err
		}
	}
	return true, nil
}

func buildRedisValue(entity Entity) string {
	encoded, _ := jsoniter.ConfigFastest.Marshal(buildLocalCacheValue(entity))
	return string(encoded)
}

func buildLocalCacheValue(entity Entity) []string {
	bind := entity.getORM().dBData
	columns := entity.getORM().tableSchema.columnNames
	length := len(columns)
	value := make([]string, length-1)
	j := 0
	for i := 1; i < length; i++ { //skip id
		v := bind[columns[i]]
		if v == nil {
			v = ""
		}
		value[j] = fmt.Sprintf("%s", v)
		j++
	}
	return value
}
