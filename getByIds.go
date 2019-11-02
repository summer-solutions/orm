package orm

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
)

func GetByIds(ids []uint64, entityName string) []interface{} {
	return GetByIdsWithReferences(ids, entityName, nil)
}

func GetByIdsWithReferences(ids []uint64, entityName string, references map[string]reflect.Type) []interface{} {
	rows, missing := TryByIdsWithReferences(ids, entityName, references)
	if len(missing) > 0 {
		panic(fmt.Errorf("entity %s not found with ids %v", entityName, missing))
	}
	return rows
}

func TryByIdsWithReferences(ids []uint64, entityName string, references map[string]reflect.Type) (found []interface{}, missing []uint64) {
	return tryByIds(ids, entityName, references)
}

func TryByIds(ids []uint64, entityName string) (found []interface{}, missing []uint64) {
	return tryByIds(ids, entityName, nil)
}

func tryByIds(ids []uint64, entityName string, references map[string]reflect.Type) (found []interface{}, missing []uint64) {
	//TODO
	lenIDs := len(ids)
	if lenIDs == 0 {
		found = make([]interface{}, 0)
		missing = make([]uint64, 0)
		return
	}

	entityType := getEntityType(entityName)
	schema := GetTableSchema(entityName)
	localCache := schema.GetLocalCacheContainer()
	redisCache := schema.GetRedisCacheContainer()
	var foundFromCache []interface{}
	var originIds []uint64
	var cacheKeys []string
	var cacheKeysMap map[string]uint64
	var cacheKeysMapReverse map[uint64]string
	if localCache != nil {
		originIds = ids
		cacheKeys = make([]string, lenIDs)
		cacheKeysMap = make(map[string]uint64, lenIDs)
		foundFromCache = make([]interface{}, 0, lenIDs)
		cacheKeysMapReverse = make(map[uint64]string, lenIDs)
		nilsFromCache := make([]uint64, 0)
		for index, id := range ids {
			cacheKey := schema.getCacheKeyLocal(id)
			cacheKeys[index] = cacheKey
			cacheKeysMap[cacheKey] = id
			cacheKeysMapReverse[id] = cacheKey
		}
		ids = make([]uint64, 0)
		for _, value := range cacheKeys {
			inCache, has := localCache.Get(value)
			if !has {
				ids = append(ids, cacheKeysMap[value])
			} else {
				if inCache != nil {
					foundFromCache = append(foundFromCache, inCache)
				} else {
					nilsFromCache = append(nilsFromCache, cacheKeysMap[value])
				}
			}
		}
		if len(ids) == 0 {
			return foundFromCache, nilsFromCache
		}
	}
	if redisCache != nil {
		originIds = ids
		cacheKeys = make([]string, lenIDs)
		cacheKeysMap = make(map[string]uint64, lenIDs)
		foundFromCache = make([]interface{}, 0, lenIDs)
		cacheKeysMapReverse = make(map[uint64]string, lenIDs)
		nilsFromCache := make([]uint64, 0)
		for index, id := range ids {
			cacheKey := schema.getCacheKeyRedis(id)
			cacheKeys[index] = cacheKey
			cacheKeysMap[cacheKey] = id
			cacheKeysMapReverse[id] = cacheKey
		}
		ids = make([]uint64, 0)
		fromCache := redisCache.MGet(cacheKeys...)
		for index, value := range fromCache {
			if value == nil {
				ids = append(ids, cacheKeysMap[cacheKeys[index]])
			} else {
				if value != "nil" {
					foundFromCache = append(foundFromCache, createEntityFromDBRow(value.(string), entityType))
				} else {
					nilsFromCache = append(nilsFromCache, cacheKeysMap[cacheKeys[index]])
				}
			}
		}
		if len(ids) == 0 {
			return foundFromCache, nilsFromCache
		}
	}
	rows := Search(NewWhere("`Id` IN ?", ids), NewPager(1, lenIDs), entityName)
	if localCache != nil || redisCache != nil {
		rows = append(rows, foundFromCache...)
		ids = originIds
	}

	length := len(rows)
	found = make([]interface{}, length)
	missing = make([]uint64, len(ids)-length)
	var index, indexMissing int
	redisValues := make([]interface{}, 0)

OUTER:
	for _, i := range ids {
		for _, row := range rows {
			id := reflect.ValueOf(row).Field(1).Uint()
			if id == i {
				found[index] = row
				if localCache != nil {
					cacheKey, has := cacheKeysMapReverse[id]
					if has {
						localCache.Set(cacheKey, row)
					}
				}
				if redisCache != nil {
					cacheKey, has := cacheKeysMapReverse[id]
					if has {
						redisValues = append(redisValues, cacheKey, buildRedisValue(row, schema))
					}
				}
				index++
				continue OUTER
			}
		}
		missing[indexMissing] = i
		indexMissing++
		if localCache != nil {
			cacheKey, has := cacheKeysMapReverse[i]
			if has {
				localCache.Set(cacheKey, nil)
			}
		}
		if redisCache != nil {
			cacheKey, has := cacheKeysMapReverse[i]
			if has {
				redisValues = append(redisValues, cacheKey, "nil")
			}
		}
	}
	if redisCache != nil && len(redisValues) > 0 {
		redisCache.MSet(redisValues...)
	}
	return
}
