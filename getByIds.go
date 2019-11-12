package orm

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
)

func GetByIds(ids []uint64, entities interface{}) {
	missing := TryByIds(ids, entities)
	if len(missing) > 0 {
		panic(fmt.Errorf("entities not found with ids %v", missing))
	}
}

func TryByIds(ids []uint64, entities interface{}) (missing []uint64) {
	return tryByIds(ids, entities)
}

func tryByIds(ids []uint64, entities interface{}) (missing []uint64) {
	originalIds := ids
	lenIDs := len(ids)
	if lenIDs == 0 {
		valOrigin := reflect.ValueOf(entities).Elem()
		valOrigin.SetLen(0)
		return make([]uint64, 0)
	}
	entityType := getEntityTypeForSlice(entities)
	schema := GetTableSchema(entityType)

	localCache := schema.GetLocalCacheContainer()
	redisCache := schema.GetRedisCacheContainer()
	contextCache := getContextCache()
	if localCache == nil && contextCache != nil {
		localCache = contextCache
	}
	var localCacheKeys []string
	var redisCacheKeys []string
	results := make(map[string]interface{}, lenIDs)
	keysMapping := make(map[string]uint64, lenIDs)
	keysReversed := make(map[uint64]string, lenIDs)
	cacheKeys := make([]string, lenIDs)
	for index, id := range ids {
		cacheKey := schema.getCacheKey(id)
		cacheKeys[index] = cacheKey
		keysMapping[cacheKey] = id
		keysReversed[id] = cacheKey
		results[cacheKey] = nil
	}

	if localCache != nil || redisCache != nil {
		if localCache != nil {
			resultsLocalCache := localCache.MGet(cacheKeys...)
			cacheKeys = getKeysForNils(schema.t, resultsLocalCache, results, false)
			localCacheKeys = cacheKeys
		}
		if redisCache != nil && len(cacheKeys) > 0 {
			resultsRedis := redisCache.MGet(cacheKeys...)
			cacheKeys = getKeysForNils(schema.t, resultsRedis, results, true)
			redisCacheKeys = cacheKeys
		}
		ids = make([]uint64, len(cacheKeys))
		for k, v := range cacheKeys {
			ids[k] = keysMapping[v]
		}
	}
	l := len(ids)
	if l > 0 {
		rows := entities
		Search(NewWhere("`Id` IN ?", ids), NewPager(1, l), rows)
		v := reflect.ValueOf(rows).Elem()
		for i := 0; i < v.Len(); i++ {
			e := v.Index(i)
			id := e.Field(1).Uint()
			results[schema.getCacheKey(id)] = e.Interface()
		}
	}
	if localCache != nil {
		l = len(localCacheKeys)
		if l > 0 {
			pairs := make([]interface{}, l*2)
			i := 0
			for _, key := range localCacheKeys {
				pairs[i] = key
				val := results[key]
				if val == nil {
					val = "nil"
				}
				pairs[i+1] = val
				i += 2
			}
			localCache.MSet(pairs...)
		}
	}

	if redisCache != nil {
		l = len(redisCacheKeys)
		if l > 0 {
			pairs := make([]interface{}, l*2)
			i := 0
			for _, key := range redisCacheKeys {
				pairs[i] = key
				val := results[key]
				if val == nil {
					val = "nil"
				} else {
					val = buildRedisValue(val, schema)
				}
				pairs[i+1] = val
				i += 2
			}
			redisCache.MSet(pairs...)
		}
	}

	missing = make([]uint64, 0)
	valOrigin := reflect.ValueOf(entities).Elem()
	valOrigin.SetLen(0)
	v := valOrigin
	for _, id := range originalIds {
		val := results[keysReversed[id]]
		if val == nil {
			missing = append(missing, id)
		} else {
			v = reflect.Append(v, reflect.ValueOf(val))
		}
	}
	valOrigin.Set(v)
	return
}

func getKeysForNils(entityType reflect.Type, rows map[string]interface{}, results map[string]interface{}, fromRedis bool) []string {
	keys := make([]string, 0)
	for k, v := range rows {
		if v == nil {
			keys = append(keys, k)
		} else {
			if v == "nil" {
				results[k] = nil
			} else if fromRedis {
				value := reflect.New(entityType).Elem()
				fillFromDBRow(v.(string), value, entityType)
				e := value.Interface()
				initIfNeeded(value, &e)
				results[k] = e
			} else {
				results[k] = v
			}
		}
	}
	return keys
}
