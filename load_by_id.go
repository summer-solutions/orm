package orm

import (
	"encoding/json"
	"fmt"
	"reflect"
)

func loadByID(engine *Engine, id uint64, entity Entity, useCache bool, references ...string) (found bool) {
	orm := initIfNeeded(engine, entity)
	schema := orm.tableSchema
	localCache, hasLocalCache := schema.GetLocalCache(engine)
	redisCache, hasRedis := schema.GetRedisCache(engine)

	if !hasLocalCache && engine.dataLoader != nil {
		e := engine.dataLoader.Load(schema, id)
		if e == nil {
			return false
		}
		fillFromDBRow(id, engine, e, entity, false)
		if len(references) > 0 {
			warmUpReferences(engine, schema, orm.elem, references, false)
		}
		return true
	}

	var cacheKey string

	if useCache {
		if !hasLocalCache && engine.hasRequestCache {
			hasLocalCache = true
			localCache = engine.GetLocalCache(requestCacheKey)
		}

		if hasLocalCache {
			cacheKey = schema.getCacheKey(id)
			e, has := localCache.Get(cacheKey)
			if has {
				if e == "nil" {
					return false
				}
				fillFromDBRow(id, engine, e.([]string), entity, false)
				if len(references) > 0 {
					warmUpReferences(engine, schema, orm.elem, references, false)
				}
				return true
			}
		}
		if hasRedis {
			cacheKey = schema.getCacheKey(id)
			row, has := redisCache.Get(cacheKey)
			if has {
				if row == "nil" {
					return false
				}
				var decoded []string
				_ = json.Unmarshal([]byte(row), &decoded)
				fillFromDBRow(id, engine, decoded, entity, false)
				if len(references) > 0 {
					warmUpReferences(engine, schema, orm.elem, references, false)
				}
				return true
			}
		}
	}

	found = searchRow(false, engine, NewWhere("`ID` = ?", id), entity, nil)
	if !found {
		if localCache != nil {
			localCache.Set(cacheKey, "nil")
		}
		if redisCache != nil {
			redisCache.Set(cacheKey, "nil", 60)
		}
		return false
	}
	if useCache {
		if localCache != nil {
			localCache.Set(cacheKey, buildLocalCacheValue(entity))
		}
		if redisCache != nil {
			redisCache.Set(cacheKey, buildRedisValue(entity), 0)
		}
	}

	if len(references) > 0 {
		warmUpReferences(engine, schema, orm.elem, references, false)
	}
	return true
}

func buildRedisValue(entity Entity) string {
	encoded, _ := json.Marshal(buildLocalCacheValue(entity))
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
			v = "nil"
		}
		value[j] = fmt.Sprintf("%s", v)
		j++
	}
	return value
}

func initIfNeeded(engine *Engine, entity Entity) *ORM {
	orm := entity.getORM()
	if orm.dBData == nil {
		value := reflect.ValueOf(entity)
		elem := value.Elem()
		t := elem.Type()
		tableSchema := getTableSchema(engine.registry, t)
		if tableSchema == nil {
			panic(fmt.Errorf("entity '%s' is not registered", t.String()))
		}
		orm.tableSchema = tableSchema
		orm.dBData = make(map[string]interface{}, len(tableSchema.columnNames))
		orm.value = value
		orm.elem = elem
		orm.idElem = elem.Field(1)
	}
	return orm
}
