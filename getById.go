package orm

import (
	"encoding/json"
	"fmt"
	"reflect"
)

func TryById(id uint64, entity interface{}, references ...string) (found bool, err error) {
	if reflect.TypeOf(entity).Kind() != reflect.Ptr {
		return false, fmt.Errorf("pointer not provided")
	}
	val := reflect.ValueOf(entity)
	elem := val.Elem()
	entityType := elem.Type()
	schema := getTableSchema(entityType)
	var cacheKey string
	localCache := schema.GetLocalCache()

	contextCache := GetContextCache()
	if localCache == nil && contextCache != nil {
		localCache = contextCache
	}

	if localCache != nil {
		cacheKey = schema.getCacheKey(id)
		e, has := localCache.Get(cacheKey)
		if has {
			if e == "nil" {
				return false, nil
			}
			err = fillFromDBRow(e.([]string), val, entityType)
			if err != nil {
				return false, err
			}
			elem.Field(1).SetUint(id)
			if len(references) > 0 {
				err = warmUpReferences(schema, elem, references, false)
				if err != nil {
					return true, err
				}
			}
			return true, nil
		}
	}
	redisCache := schema.GetRedisCacheContainer()
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
			err = fillFromDBRow(decoded, val, entityType)
			if err != nil {
				return false, err
			}
			if len(references) > 0 {
				err = warmUpReferences(schema, elem, references, false)
				if err != nil {
					return true, err
				}
			}
			return true, nil
		}
	}
	found, err = SearchOne(NewWhere("`Id` = ?", id), entity)
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
		err = warmUpReferences(schema, elem, references, false)
		if err != nil {
			return true, err
		}
	}
	return true, nil
}

func GetById(id uint64, entity interface{}, references ...string) error {
	found, err := TryById(id, entity, references...)
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
