package orm

import (
	"fmt"
	"reflect"
	"strings"
)

func TryById(id uint64, entityName string) (entity interface{}, found bool) {
	entityType := getEntityType(entityName)
	schema := GetTableSchema(entityName)
	var cacheKey string
	localCache := schema.GetLocalCacheContainer()

	contextCache := getContentCache()
	if localCache == nil && contextCache != nil {
		localCache = contextCache
	}

	if localCache != nil {
		cacheKey = schema.getCacheKey(id)
		entity, has := localCache.Get(cacheKey)
		if has {
			if entity == nil {
				return entity, false
			}
			return entity, true
		}
	}
	redisCache := schema.GetRedisCacheContainer()
	if redisCache != nil {
		cacheKey = schema.getCacheKey(id)
		row, has := redisCache.Get(cacheKey)
		if has {
			if row == "nil" {
				return nil, false
			}
			entity := createEntityFromDBRow(row, entityType)
			return entity, true
		}
	}

	rows := Search(NewWhere("`Id` = ?", id), NewPager(1, 1), entityName)
	if len(rows) == 0 {
		if localCache != nil {
			localCache.Set(cacheKey, nil)
		}
		return nil, false
	}
	if localCache != nil {
		localCache.Set(cacheKey, rows[0])
	}
	if redisCache != nil {
		redisCache.Set(cacheKey, buildRedisValue(rows[0], schema), 0)
	}
	return rows[0], true
}

func GetById(id uint64, entityName string) interface{} {
	entity, found := TryById(id, entityName)
	if !found {
		panic(fmt.Errorf("entity %s with id %d not found", entityName, id))
	}
	return entity
}

func buildRedisValue(entity interface{}, schema *TableSchema) string {
	bind := reflect.ValueOf(entity).Field(0).Interface().(ORM).DBData
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
