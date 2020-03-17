package orm

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

func GetByIds(ids []uint64, entities interface{}, references ...string) error {
	missing, err := TryByIds(ids, entities, references...)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return fmt.Errorf("entities not found with ids %v", missing)
	}
	return nil
}

func TryByIds(ids []uint64, entities interface{}, references ...string) (missing []uint64, err error) {
	return tryByIds(ids, reflect.ValueOf(entities).Elem(), references)
}

func tryByIds(ids []uint64, entities reflect.Value, references []string) (missing []uint64, err error) {
	originalIds := ids
	lenIDs := len(ids)
	if lenIDs == 0 {
		entities.SetLen(0)
		return make([]uint64, 0), nil
	}
	schema := getTableSchema(getEntityTypeForSlice(entities.Type()))

	localCache := schema.GetLocalCache()
	redisCache := schema.GetRedisCacheContainer()
	contextCache := GetContextCache()
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
			cacheKeys, err = getKeysForNils(schema.t, resultsLocalCache, results, false)
			if err != nil {
				return nil, err
			}
			localCacheKeys = cacheKeys
		}
		if redisCache != nil && len(cacheKeys) > 0 {
			resultsRedis, err := redisCache.MGet(cacheKeys...)
			if err != nil {
				return nil, err
			}
			cacheKeys, err = getKeysForNils(schema.t, resultsRedis, results, true)
			if err != nil {
				return nil, err
			}
			redisCacheKeys = cacheKeys
		}
		ids = make([]uint64, len(cacheKeys))
		for k, v := range cacheKeys {
			ids[k] = keysMapping[v]
		}
	}
	l := len(ids)
	if l > 0 {
		_, err = search(NewWhere("`Id` IN ?", ids), &Pager{1, l}, false, entities)
		if err != nil {
			return nil, err
		}
		for i := 0; i < entities.Len(); i++ {
			e := entities.Index(i)
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
			err = redisCache.MSet(pairs...)
			if err != nil {
				return nil, err
			}
		}
	}

	missing = make([]uint64, 0)
	valOrigin := entities
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
	if len(references) > 0 && v.Len() > 0 {
		err = warmUpReferences(schema, entities, references)
		if err != nil {
			return nil, err
		}
	}
	return
}

func getKeysForNils(entityType reflect.Type, rows map[string]interface{}, results map[string]interface{}, fromRedis bool) ([]string, error) {
	keys := make([]string, 0)
	for k, v := range rows {
		if v == nil {
			keys = append(keys, k)
		} else {
			if v == "nil" {
				results[k] = nil
			} else if fromRedis {
				value := reflect.New(entityType).Elem()

				var decoded []string
				err := json.Unmarshal([]byte(v.(string)), &decoded)
				if err != nil {
					return nil, err
				}

				err = fillFromDBRow(decoded, value, entityType)
				if err != nil {
					return nil, err
				}
				e := value.Interface()
				_, err = initIfNeeded(value, &e)
				if err != nil {
					return nil, err
				}
				results[k] = e
			} else {
				results[k] = v
			}
		}
	}
	return keys, nil
}

func warmUpReferences(tableSchema *TableSchema, rows reflect.Value, references []string) error {
	warmUpRows := make(map[reflect.Type]map[uint64]bool)
	warmUpRefs := make(map[reflect.Type]map[uint64]*ReferenceOne)
	warmUpRowsIds := make(map[reflect.Type][]uint64)
	warmUpSubRefs := make(map[reflect.Type][]string)
	l := rows.Len()
	for _, ref := range references {
		parts := strings.Split(ref, "/")
		_, has := tableSchema.tags[parts[0]]
		if !has {
			return fmt.Errorf("invalid reference %s", ref)
		}
		parentRef, has := tableSchema.tags[parts[0]]["ref"]
		if !has {
			return fmt.Errorf("missing reference tag %s", ref)
		}
		parentType := GetEntityType(parentRef)
		warmUpSubRefs[parentType] = append(warmUpSubRefs[parentType], parts[1:]...)

		for i := 0; i < l; i++ {
			ref := rows.Index(i).FieldByName(parts[0]).Interface()
			ids := make([]uint64, 0)
			oneRef, ok := ref.(*ReferenceOne)
			if ok {
				if oneRef.Id != 0 {
					ids = append(ids, oneRef.Id)
					if warmUpRefs[parentType] == nil {
						warmUpRefs[parentType] = make(map[uint64]*ReferenceOne)
					}
					warmUpRefs[parentType][oneRef.Id] = oneRef
				}
			} else {
				manyRef, ok := ref.(*ReferenceMany)
				if ok {
					if manyRef.Ids != nil {
						ids = append(ids, manyRef.Ids...)
						//TODO add warmUpRefs
					}
				}
			}
			if len(ids) == 0 {
				continue
			}
			if warmUpRows[parentType] == nil {
				warmUpRows[parentType] = make(map[uint64]bool)
				warmUpRowsIds[parentType] = make([]uint64, 0)
			}
			for _, id := range ids {
				_, has := warmUpRows[parentType][id]
				if !has {
					warmUpRows[parentType][id] = true
					warmUpRowsIds[parentType] = append(warmUpRowsIds[parentType], id)
				}
			}

		}
	}
	for t, ids := range warmUpRowsIds {
		sub := reflect.New(reflect.SliceOf(t)).Elem()
		_, err := tryByIds(ids, sub, warmUpSubRefs[t])
		if err != nil {
			return err
		}
		subLen := sub.Len()
		for i := 0; i < subLen; i++ {
			v := sub.Index(i)
			id := v.Field(1).Uint()
			ref, has := warmUpRefs[t][id]
			if has {
				ref.Reference = v.Interface()
			}
		}
	}
	return nil
}
