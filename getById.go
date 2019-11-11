package orm

import (
	"fmt"
	"reflect"
	"strings"
)

func TryById(id uint64, entity interface{}) (found bool) {
	if reflect.TypeOf(entity).Kind() != reflect.Ptr {
		panic(fmt.Errorf("pointer not provided"))
	}
	entityType := reflect.ValueOf(entity).Elem().Type()
	schema := GetTableSchema(entityType)
	var cacheKey string
	localCache := schema.GetLocalCacheContainer()

	contextCache := getContextCache()
	if localCache == nil && contextCache != nil {
		localCache = contextCache
	}

	if localCache != nil {
		cacheKey = schema.getCacheKey(id)
		entity, has := localCache.Get(cacheKey)
		if has {
			if entity == nil {
				return false
			}
			return true
		}
	}
	redisCache := schema.GetRedisCacheContainer()
	if redisCache != nil {
		cacheKey = schema.getCacheKey(id)
		row, has := redisCache.Get(cacheKey)
		if has {
			if row == "nil" {
				return false
			}
			val := reflect.ValueOf(entity).Elem()
			fillFromDBRow(row, val, entityType)
			entity = val.Interface()
			return true
		}
	}
	found = SearchOne(NewWhere("`Id` = ?", id), entity)
	if !found {
		if localCache != nil {
			localCache.Set(cacheKey, nil)
		}
		return false
	}
	if localCache != nil {
		localCache.Set(cacheKey, entity)
	}
	if redisCache != nil {
		redisCache.Set(cacheKey, buildRedisValue(entity, schema), 0)
	}
	return true
}

func GetById(id uint64, entity interface{}) {
	found := TryById(id, entity)
	if !found {
		panic(fmt.Errorf("entity %T with id %d not found", entity, id))
	}
}

func buildRedisValue(entity interface{}, schema *TableSchema) string {
	bind := reflect.Indirect(reflect.ValueOf(entity)).Field(0).Interface().(ORM).dBData
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
