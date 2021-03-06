package orm

import (
	"fmt"
	"reflect"

	jsoniter "github.com/json-iterator/go"
)

func loadByID(engine *Engine, id uint64, entity Entity, fillStruct bool, useCache bool, references ...string) (found bool, data []interface{}, schema *tableSchema) {
	var orm *ORM
	if fillStruct {
		orm = initIfNeeded(engine, entity)
		schema = orm.tableSchema
	} else {
		schema = engine.registry.GetTableSchemaForEntity(entity).(*tableSchema)
	}
	localCache, hasLocalCache := schema.GetLocalCache(engine)
	redisCache, hasRedis := schema.GetRedisCache(engine)
	if !hasLocalCache && engine.dataLoader != nil {
		e := engine.dataLoader.Load(schema, id)
		if e == nil {
			return false, nil, schema
		}
		if fillStruct {
			fillFromDBRow(id, engine, e, entity, false)
		} else {
			e[0] = id
		}
		if len(references) > 0 {
			warmUpReferences(engine, schema, orm.elem, references, false)
		}
		return true, e, schema
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
					return false, nil, schema
				}
				data := e.([]interface{})
				if fillStruct {
					fillFromDBRow(id, engine, data, entity, false)
				} else {
					data[0] = id
				}
				if len(references) > 0 {
					warmUpReferences(engine, schema, orm.elem, references, false)
				}
				return true, data, schema
			}
		}
		if hasRedis {
			cacheKey = schema.getCacheKey(id)
			row, has := redisCache.Get(cacheKey)
			if has {
				if row == "nil" {
					return false, nil, schema
				}
				decoded := make([]interface{}, len(schema.columnNames))
				_ = jsoniter.ConfigFastest.Unmarshal([]byte(row), &decoded)
				convertDataFromJSON(schema.fields, 0, decoded)
				if fillStruct {
					fillFromDBRow(id, engine, decoded, entity, false)
				} else {
					decoded[0] = id
				}
				if len(references) > 0 {
					warmUpReferences(engine, schema, orm.elem, references, false)
				}
				return true, decoded, schema
			}
		}
	}

	found, data = searchRow(false, fillStruct, engine, NewWhere("`ID` = ?", id), entity, nil)
	if !found {
		if localCache != nil {
			localCache.Set(cacheKey, "nil")
		}
		if redisCache != nil {
			redisCache.Set(cacheKey, "nil", 60)
		}
		return false, nil, schema
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
	} else {
		data[0] = id
	}
	return true, data, schema
}

func buildRedisValue(entity Entity) string {
	encoded, _ := jsoniter.ConfigFastest.Marshal(buildLocalCacheValue(entity))
	return string(encoded)
}

func buildLocalCacheValue(entity Entity) []interface{} {
	data := entity.getORM().dBData
	b := make([]interface{}, len(data))
	copy(b, data)
	return b
}

func initIfNeeded(engine *Engine, entity Entity) *ORM {
	orm := entity.getORM()
	if !orm.initialised {
		orm.initialised = true
		value := reflect.ValueOf(entity)
		elem := value.Elem()
		t := elem.Type()
		tableSchema := getTableSchema(engine.registry, t)
		if tableSchema == nil {
			panic(fmt.Errorf("entity '%s' is not registered", t.String()))
		}
		orm.tableSchema = tableSchema
		orm.value = value
		orm.elem = elem
		orm.idElem = elem.Field(1)
	}
	return orm
}

func convertDataFromJSON(fields *tableFields, start int, encoded []interface{}) int {
	for i := 0; i < len(fields.uintegers); i++ {
		encoded[start] = uint64(encoded[start].(float64))
		start++
	}
	for i := 0; i < len(fields.uintegersNullable); i++ {
		v := encoded[start]
		if v != nil {
			encoded[start] = uint64(v.(float64))
		}
		start++
	}
	for i := 0; i < len(fields.integers); i++ {
		encoded[start] = int64(encoded[start].(float64))
		start++
	}
	for i := 0; i < len(fields.integersNullable); i++ {
		v := encoded[start]
		if v != nil {
			encoded[start] = int64(v.(float64))
		}
		start++
	}
	start += len(fields.strings) + len(fields.sliceStrings) + len(fields.bytes)
	if fields.fakeDelete > 0 {
		encoded[start] = uint64(encoded[start].(float64))
		start++
	}
	start += len(fields.booleans) + len(fields.booleansNullable) + len(fields.floats) + len(fields.floatsNullable) +
		len(fields.timesNullable) + len(fields.times) + len(fields.jsons)
	for i := 0; i < len(fields.refs); i++ {
		v := encoded[start]
		if v != nil {
			encoded[start] = uint64(v.(float64))
		}
		start++
	}
	start += len(fields.refsMany)
	for _, subFields := range fields.structs {
		start = convertDataFromJSON(subFields, start, encoded)
	}
	return start
}
