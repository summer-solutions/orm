package orm

import (
	"fmt"
	"reflect"
	"strings"
)

func TryById(id uint64, entity interface{}) (found bool, err error) {
	if reflect.TypeOf(entity).Kind() != reflect.Ptr {
		return false, fmt.Errorf("pointer not provided")
	}
	entityType := reflect.ValueOf(entity).Elem().Type()
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
			if e == nil {
				return false, nil
			}
			valEntity := reflect.ValueOf(entity).Elem()
			valLocal := reflect.ValueOf(e)
			for i := 0; i < valEntity.NumField(); i++ {
				valEntity.Field(i).Set(valLocal.Field(i))
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
			val := reflect.ValueOf(entity).Elem()
			err = fillFromDBRow(row, val, entityType)
			if err != nil {
				return false, err
			}
			_, err := initIfNeeded(val, entity)
			if err != nil {
				return false, err
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
			localCache.Set(cacheKey, nil)
		}
		return false, nil
	}
	if localCache != nil {
		localCache.Set(cacheKey, reflect.Indirect(reflect.ValueOf(entity)).Interface())
	}
	if redisCache != nil {
		err = redisCache.Set(cacheKey, buildRedisValue(entity, schema), 0)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func GetById(id uint64, entity interface{}) error {
	found, err := TryById(id, entity)
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
	return strings.Join(value, "|")
}
