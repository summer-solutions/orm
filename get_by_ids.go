package orm

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

var nilValue = reflect.Value{}

func tryByIDs(engine *Engine, ids []uint64, entities reflect.Value, references []string) (missing []uint64, err error) {
	originalIDs := ids
	lenIDs := len(ids)
	if lenIDs == 0 {
		entities.SetLen(0)
		return make([]uint64, 0), nil
	}
	t, has := getEntityTypeForSlice(engine.registry, entities.Type())
	if !has {
		return nil, EntityNotRegisteredError{Name: entities.Type().String()}
	}

	schema := getTableSchema(engine.registry, t)
	localCache, hasLocalCache := schema.GetLocalCache(engine)
	redisCache, hasRedis := schema.GetRedisCache(engine)
	var localCacheKeys []string
	var redisCacheKeys []string
	results := make(map[string]reflect.Value, lenIDs)
	keysMapping := make(map[string]uint64, lenIDs)
	keysReversed := make(map[uint64]string, lenIDs)
	cacheKeys := make([]string, lenIDs)
	for index, id := range ids {
		cacheKey := schema.getCacheKey(id)
		cacheKeys[index] = cacheKey
		keysMapping[cacheKey] = id
		keysReversed[id] = cacheKey
		results[cacheKey] = nilValue
	}

	if hasLocalCache || hasRedis {
		if hasLocalCache {
			resultsLocalCache := localCache.MGet(cacheKeys...)
			cacheKeys, err = getKeysForNils(engine, schema.t, resultsLocalCache, keysMapping, results, false)
			if err != nil {
				return nil, err
			}
			localCacheKeys = cacheKeys
		}
		if hasRedis && len(cacheKeys) > 0 {
			resultsRedis, err := redisCache.MGet(cacheKeys...)
			if err != nil {
				return nil, err
			}
			cacheKeys, err = getKeysForNils(engine, schema.t, resultsRedis, keysMapping, results, true)
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
		_, err = search(false, engine, NewWhere("`ID` IN ?", ids), &Pager{1, l}, false, entities)
		if err != nil {
			return nil, err
		}
		for i := 0; i < entities.Len(); i++ {
			e := entities.Index(i)
			id := e.Elem().Field(1).Uint()
			results[schema.getCacheKey(id)] = e
		}
	}
	if hasLocalCache {
		l = len(localCacheKeys)
		if l > 0 {
			pairs := make([]interface{}, l*2)
			i := 0
			for _, key := range localCacheKeys {
				pairs[i] = key
				val := results[key]
				var toSet interface{}
				if val == nilValue {
					toSet = "nil"
				} else {
					toSet = buildLocalCacheValue(val.Interface().(Entity))
				}
				pairs[i+1] = toSet
				i += 2
			}
			localCache.MSet(pairs...)
		}
	}

	if hasRedis {
		l = len(redisCacheKeys)
		if l > 0 {
			pairs := make([]interface{}, l*2)
			i := 0
			for _, key := range redisCacheKeys {
				pairs[i] = key
				val := results[key]
				var toSet interface{}
				if val == nilValue {
					toSet = "nil"
				} else {
					toSet = buildRedisValue(val.Interface().(Entity))
				}
				pairs[i+1] = toSet
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
	valOrigin.SetCap(0)
	v := valOrigin
	for _, id := range originalIDs {
		val := results[keysReversed[id]]
		if val == nilValue {
			missing = append(missing, id)
		} else {
			v = reflect.Append(v, val)
		}
	}
	valOrigin.Set(v)
	if len(references) > 0 && v.Len() > 0 {
		err = warmUpReferences(engine, schema, entities, references, true)
		if err != nil {
			return nil, err
		}
	}
	return
}

func getKeysForNils(engine *Engine, entityType reflect.Type, rows map[string]interface{}, keysMapping map[string]uint64,
	results map[string]reflect.Value, fromRedis bool) ([]string, error) {
	keys := make([]string, 0)
	for k, v := range rows {
		if v == nil {
			keys = append(keys, k)
		} else {
			if v == "nil" {
				results[k] = nilValue
			} else if fromRedis {
				value := reflect.New(entityType)
				var decoded []string
				err := json.Unmarshal([]byte(v.(string)), &decoded)
				if err != nil {
					return nil, err
				}
				err = fillFromDBRow(keysMapping[k], engine, decoded, value, entityType)
				if err != nil {
					return nil, err
				}
				results[k] = value
			} else {
				value := reflect.New(entityType)
				err := fillFromDBRow(keysMapping[k], engine, v.([]string), value, entityType)
				if err != nil {
					return nil, err
				}
				results[k] = value
			}
		}
	}
	return keys, nil
}

func warmUpReferences(engine *Engine, tableSchema *tableSchema, rows reflect.Value, references []string, many bool) error {
	warmUpRows := make(map[reflect.Type]map[uint64]bool)
	warmUpRefs := make(map[reflect.Type]map[uint64][]reflect.Value)
	warmUpRowsIDs := make(map[reflect.Type][]uint64)
	warmUpSubRefs := make(map[reflect.Type][]string)
	l := 1
	if many {
		l = rows.Len()
	}
	if references[0] == "*" {
		references = tableSchema.refOne
	}
	for _, ref := range references {
		parts := strings.Split(ref, "/")
		_, has := tableSchema.tags[parts[0]]
		if !has {
			return fmt.Errorf("invalid reference %s in %s", ref, tableSchema.tableName)
		}
		parentRef, has := tableSchema.tags[parts[0]]["ref"]
		if !has {
			return fmt.Errorf("missing reference tag %s", ref)
		}
		parentType, has := engine.registry.entities[parentRef]
		if !has {
			return EntityNotRegisteredError{Name: tableSchema.tags[parts[0]]["ref"]}
		}
		newSub := parts[1:]
		if len(newSub) > 0 {
			warmUpSubRefs[parentType] = append(warmUpSubRefs[parentType], strings.Join(newSub, "/"))
		}

		for i := 0; i < l; i++ {
			var ref reflect.Value
			if many {
				ref = rows.Index(i).Elem().FieldByName(parts[0])
			} else {
				ref = rows.FieldByName(parts[0])
			}
			if ref.IsZero() {
				continue
			}
			ref = ref.Elem()
			refID := ref.Field(1).Uint()
			ids := make([]uint64, 0)
			if refID != 0 {
				ids = append(ids, refID)
				if warmUpRefs[parentType] == nil {
					warmUpRefs[parentType] = make(map[uint64][]reflect.Value)
				}
				if warmUpRefs[parentType][refID] == nil {
					warmUpRefs[parentType][refID] = make([]reflect.Value, 0)
				}
				warmUpRefs[parentType][refID] = append(warmUpRefs[parentType][refID], ref)
			}
			if len(ids) == 0 {
				continue
			}
			if warmUpRows[parentType] == nil {
				warmUpRows[parentType] = make(map[uint64]bool)
				warmUpRowsIDs[parentType] = make([]uint64, 0)
			}
			for _, id := range ids {
				_, has := warmUpRows[parentType][id]
				if !has {
					warmUpRows[parentType][id] = true
					warmUpRowsIDs[parentType] = append(warmUpRowsIDs[parentType], id)
				}
			}
		}
	}
	for t, ids := range warmUpRowsIDs {
		sub := reflect.New(reflect.SliceOf(reflect.PtrTo(t))).Elem()
		_, err := tryByIDs(engine, ids, sub, warmUpSubRefs[t])
		if err != nil {
			return err
		}
		subLen := sub.Len()
		for i := 0; i < subLen; i++ {
			v := sub.Index(i)
			id := v.Elem().Field(1).Uint()
			refs, has := warmUpRefs[t][id]
			if has {
				for _, ref := range refs {
					ref.Set(v.Elem())
				}
			}
		}
	}
	return nil
}
