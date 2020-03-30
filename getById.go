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
	schema := getTableSchema(engine.config, entityType)
	var cacheKey string
	localCache := schema.GetLocalCache(engine)

	if localCache != nil {
		cacheKey = schema.getCacheKey(id)
		e, has := localCache.Get(cacheKey)
		if has {
			if e == "nil" {
				return false, nil
			}
			err = fillFromDBRow(engine, e.([]string), val, entityType)
			if err != nil {
				return false, err
			}
			elem.Field(1).SetUint(id)
			if len(references) > 0 {
				err = warmUpReferences(engine, schema, elem, references, false)
				if err != nil {
					return true, err
				}
			}
			return true, nil
		}
	}
	redisCache := schema.GetRedisCacheContainer(engine)
	if redisCache != nil {
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
			err = fillFromDBRow(engine, decoded, val, entityType)
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
	found, err = engine.SearchOne(NewWhere("`Id` = ?", id), entity)
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
	value := make([]string, length)
	for i := 0; i < length; i++ {
		v := bind[schema.columnNames[i]]
		if v == nil {
			v = ""
		}
		value[i] = fmt.Sprintf("%s", v)
	}
	encoded, _ := json.Marshal(value)
	return string(encoded)
}

func buildLocalCacheValue(entity interface{}, schema *TableSchema) []string {
	bind := reflect.Indirect(reflect.ValueOf(entity)).Field(0).Interface().(*ORM).dBData
	length := len(schema.columnNames)
	value := make([]string, length)
	for i := 0; i < length; i++ {
		v := bind[schema.columnNames[i]]
		if v == nil {
			v = ""
		}
		value[i] = fmt.Sprintf("%s", v)
	}
	return value
}
