package orm

import (
	"encoding/json"
	"fmt"
	"reflect"
)

func loadByID(engine *Engine, id uint64, entity interface{}, useCache bool, references ...string) (found bool, err error) {
	val := reflect.ValueOf(entity)
	if val.Kind() != reflect.Ptr {
		panic(fmt.Errorf("entity '%s' is not a poninter", val.Type().String()))
	}
	elem := val.Elem()
	if !elem.IsValid() {
		panic(fmt.Errorf("entity '%s' is not a addressable", val.Type().String()))
	}
	entityType := elem.Type()
	for {
		if entityType.Kind() == reflect.Ptr {
			entityType = entityType.Elem()
			val = val.Elem()
			elem = val.Elem()
		} else {
			break
		}
	}
	schema := getTableSchema(engine.registry, entityType)
	if schema == nil {
		return false, EntityNotRegisteredError{Name: entityType.String()}
	}
	var cacheKey string
	localCache, hasLocalCache := schema.GetLocalCache(engine)

	if hasLocalCache && useCache {
		cacheKey = schema.getCacheKey(id)
		e, has := localCache.Get(cacheKey)
		if has {
			if e == "nil" {
				return false, nil
			}
			err = fillFromDBRow(id, engine, e.([]string), val, entityType)
			if err != nil {
				return false, err
			}
			if len(references) > 0 {
				err = warmUpReferences(engine, schema, elem, references, false)
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
			err = json.Unmarshal([]byte(row), &decoded)
			if err != nil {
				return true, err
			}
			err = fillFromDBRow(id, engine, decoded, val, entityType)
			if err != nil {
				return false, err
			}
			if len(references) > 0 {
				err = warmUpReferences(engine, schema, elem, references, false)
			}
			return true, err
		}
	}
	found, err = searchRow(false, engine, NewWhere("`ID` = ?", id), val, nil)
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
		localCache.Set(cacheKey, buildLocalCacheValue(elem, schema))
	}
	if redisCache != nil && useCache {
		err = redisCache.Set(cacheKey, buildRedisValue(elem, schema), 0)
		if err != nil {
			return false, err
		}
	}
	if len(references) > 0 {
		err = warmUpReferences(engine, schema, elem, references, false)
		if err != nil {
			return true, err
		}
	}
	return true, nil
}

func buildRedisValue(elem reflect.Value, schema *tableSchema) string {
	encoded, _ := json.Marshal(buildLocalCacheValue(elem, schema))
	return string(encoded)
}

func buildLocalCacheValue(elem reflect.Value, schema *tableSchema) []string {
	bind := elem.Interface().(Entity).getORM().dBData
	length := len(schema.columnNames)
	value := make([]string, length-1)
	j := 0
	for i := 1; i < length; i++ { //skip id
		v := bind[schema.columnNames[i]]
		if v == nil {
			v = ""
		}
		value[j] = fmt.Sprintf("%s", v)
		j++
	}
	return value
}
