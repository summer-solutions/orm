package orm

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
	"strings"
)

func GetByIds(ids []uint64, entityName string, references ...string) []interface{} {
	rows, missing := TryByIds(ids, entityName, references...)
	if len(missing) > 0 {
		panic(fmt.Errorf("entity %s not found with ids %v", entityName, missing))
	}
	return rows
}

func TryByIds(ids []uint64, entityName string, references ...string) (found []interface{}, missing []uint64) {
	return tryByIds(ids, entityName, references...)
}

func tryByIds(ids []uint64, entityName string, references ...string) (found []interface{}, missing []uint64) {
	originalIds := ids
	lenIDs := len(ids)
	if lenIDs == 0 {
		return make([]interface{}, 0), make([]uint64, 0)
	}
	schema := GetTableSchema(entityName)

	localCache := schema.GetLocalCacheContainer()
	redisCache := schema.GetRedisCacheContainer()
	contextCache := getContentCache()
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
		rows := Search(NewWhere("`Id` IN ?", ids), NewPager(1, l), entityName)
		for _, row := range rows {
			id := reflect.ValueOf(row).Field(1).Uint()
			results[schema.getCacheKey(id)] = row
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
	found = make([]interface{}, 0, lenIDs)
	for _, id := range originalIds {
		val := results[keysReversed[id]]
		if val == nil {
			missing = append(missing, id)
		} else {
			found = append(found, val)
		}
	}
	warmUpReferences(schema, found, references)
	return
}

func warmUpReferences(tableSchema *TableSchema, rows []interface{}, references []string) {
	if len(references) == 0 || len(rows) == 0 {
		return
	}
	warmUpRows := make(map[reflect.Type]map[uint64]bool)
	warmUpRowsIds := make(map[reflect.Type][]uint64)
	warmUpSubRefs := make(map[reflect.Type][]string)
	for _, ref := range references {
		parts := strings.Split(ref, "/")
		_, has := tableSchema.tags[parts[0]]
		if !has {
			panic(fmt.Errorf("invalid reference %s", ref))
		}
		parentRef, has := tableSchema.tags[parts[0]]["ref"]
		if !has {
			panic(fmt.Errorf("missing reference tag %s", ref))
		}
		parentType := getEntityType(parentRef)
		warmUpSubRefs[parentType] = append(warmUpSubRefs[parentType], parts[1:]...)
		for _, entity := range rows {
			id := reflect.ValueOf(entity).FieldByName(parts[0]).Uint()
			if id == 0 {
				continue
			}
			if warmUpRows[parentType] == nil {
				warmUpRows[parentType] = make(map[uint64]bool)
				warmUpRowsIds[parentType] = make([]uint64, 0)
			}
			_, has := warmUpRows[parentType][id]
			if !has {
				warmUpRows[parentType][id] = true
				warmUpRowsIds[parentType] = append(warmUpRowsIds[parentType], id)
			}
		}
	}
	for t, ids := range warmUpRowsIds {
		GetByIds(ids, t.String(), warmUpSubRefs[t]...)
	}
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
				results[k] = createEntityFromDBRow(v.(string), entityType)
			} else {
				results[k] = v
			}
		}
	}
	return keys
}
