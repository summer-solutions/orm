package orm

import (
	"encoding/json"
	"fmt"
	"reflect"
)

func tryById(engine *Engine, id uint64, entity interface{}, references ...string) (found bool, err error) {
	val := reflect.ValueOf(entity)
	elem := val.Elem()
	entityType := elem.Type()
	schema, has, err := getTableSchema(engine.config, entityType)
	if err != nil {
		return false, err
	}
	if !has {
		return false, EntityNotRegisteredError{Name: entityType.String()}
	}
	var cacheKey string
	localCache, hasLocalCache := schema.GetLocalCache(engine)

	if hasLocalCache {
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
				if err != nil {
					return true, err
				}
			}
			return true, nil
		}
	}
	redisCache, hasRedis := schema.GetRedisCacheContainer(engine)
	if hasRedis {
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
				if err != nil {
					return true, err
				}
			}
			return true, nil
		}
	}
	found, err = searchOne(false, engine, NewWhere("`Id` = ?", id), entity)
	if err != nil {
		return false, err
	}
	if !found {
		if localCache != nil {
			localCache.Set(cacheKey, "nil")
		}
		return false, nil
	}
	if localCache != nil {
		localCache.Set(cacheKey, buildLocalCacheValue(entity, schema))
	}
	if redisCache != nil {
		err = redisCache.Set(cacheKey, buildRedisValue(entity, schema), 0)
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

func getById(engine *Engine, id uint64, entity interface{}, references ...string) error {
	found, err := engine.TryById(id, entity, references...)
	if err != nil {
		return err
	}
	if !found {
		return fmt.Errorf("entity %T with id %d not found", entity, id)
	}
	return nil
}

func buildRedisValue(entity interface{}, schema *TableSchema) string {
	bind := reflect.Indirect(reflect.ValueOf(entity)).Field(0).Interface().(*ORM).dBData
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
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func buildLocalCacheValue(entity interface{}, schema *TableSchema) []string {
	bind := reflect.Indirect(reflect.ValueOf(entity)).Field(0).Interface().(*ORM).dBData
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
