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
	lenIDs := len(ids)
	if lenIDs == 0 {
		found = make([]interface{}, 0)
		missing = make([]uint64, 0)
		return
	}

	idsMapping := make(map[uint64]int, lenIDs) //TODO use is to refactor order
	for index, id := range ids {
		idsMapping[id] = index
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

	contextCache := getContentCache()
	if localCache == nil && contextCache != nil {
		localCache = contextCache
	}

	var missingInLocalCache []uint64
	if localCache != nil {
		missingInLocalCache = make([]uint64, 0)
		originIds = ids
		cacheKeys = make([]string, lenIDs)
		cacheKeysMap = make(map[string]uint64, lenIDs)
		foundFromCache = make([]interface{}, 0, lenIDs)
		cacheKeysMapReverse = make(map[uint64]string, lenIDs)
		nilsFromCache := make([]uint64, 0)
		for index, id := range ids {
			cacheKey := schema.getCacheKey(id)
			cacheKeys[index] = cacheKey
			cacheKeysMap[cacheKey] = id
			cacheKeysMapReverse[id] = cacheKey
		}
		ids = make([]uint64, 0)

		allInCache := localCache.MGet(cacheKeys...)

		for index, value := range allInCache {

			if value == nil {
				id := cacheKeysMap[cacheKeys[index]]
				ids = append(ids, id)
				missingInLocalCache = append(missingInLocalCache, id)

			} else {
				if value != "nil" {
					foundFromCache = append(foundFromCache, value)
				} else {
					nilsFromCache = append(nilsFromCache, cacheKeysMap[cacheKeys[index]])
				}
			}
		}
		if len(ids) == 0 {
			warmUpReferences(schema, foundFromCache, references)
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
			cacheKey := schema.getCacheKey(id)
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

			lenMissing := len(missingInLocalCache)
			if localCache != nil && lenMissing > 0 {
				pairs := make([]interface{}, lenMissing*2)
				i := 0
				for _, id := range missingInLocalCache {
					pairs[i] = cacheKeysMapReverse[id]
					pairs[i+1] = foundFromCache[idsMapping[id]]
					i += 2
				}
				localCache.MSet(pairs...)
			}

			warmUpReferences(schema, foundFromCache, references)
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
						localCache.Set(cacheKey, row) //TODO use mset AND only set if not exists in local cache
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
				localCache.Set(cacheKey, "nil") //TODO use mset  AND only set if not exists in local cache
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
