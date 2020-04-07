package orm

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
)

func getByIds(engine *Engine, ids []uint64, entities interface{}, references ...string) error {
	missing, err := engine.TryByIds(ids, entities, references...)
	if err != nil {
		return err
	}
	if len(missing) > 0 {
		return fmt.Errorf("entities not found with ids %v", missing)
	}
	return nil
}

func tryByIds(engine *Engine, ids []uint64, entities reflect.Value, references []string) (missing []uint64, err error) {
	originalIds := ids
	lenIDs := len(ids)
	if lenIDs == 0 {
		entities.SetLen(0)
		return make([]uint64, 0), nil
	}
	t, has := getEntityTypeForSlice(engine.config, entities.Type())
	if !has {
		return nil, EntityNotRegisteredError{Name: entities.Type().String()}

	}

	schema := getTableSchema(engine.config, t)
	localCache, hasLocalCache := schema.GetLocalCache(engine)
	redisCache, hasRedis := schema.GetRedisCacheContainer(engine)
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
		_, err = search(false, engine, NewWhere("`Id` IN ?", ids), &Pager{1, l}, false, entities)
		if err != nil {
			return nil, err
		}
		for i := 0; i < entities.Len(); i++ {
			e := entities.Index(i)
			id := e.Elem().Field(1).Uint()
			results[schema.getCacheKey(id)] = e.Interface()
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
				if val == nil {
					val = "nil"
				} else {
					val = buildLocalCacheValue(val, schema)
				}
				pairs[i+1] = val
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
			e := reflect.ValueOf(val)
			v = reflect.Append(v, e)
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
	results map[string]interface{}, fromRedis bool) ([]string, error) {
	keys := make([]string, 0)
	for k, v := range rows {
		if v == nil {
			keys = append(keys, k)
		} else {
			if v == "nil" {
				results[k] = nil
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
				results[k] = value.Interface()
			} else {
				value := reflect.New(entityType)
				err := fillFromDBRow(keysMapping[k], engine, v.([]string), value, entityType)
				if err != nil {
					return nil, err
				}
				results[k] = value.Interface()
			}
		}
	}
	return keys, nil
}

func warmUpReferences(engine *Engine, tableSchema *TableSchema, rows reflect.Value, references []string, many bool) error {
	warmUpRows := make(map[reflect.Type]map[uint64]bool)
	warmUpRefs := make(map[reflect.Type]map[uint64][]*ReferenceOne)
	warmUpRowsIds := make(map[reflect.Type][]uint64)
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
		_, has := tableSchema.Tags[parts[0]]
		if !has {
			return fmt.Errorf("invalid reference %s", ref)
		}
		parentRef, has := tableSchema.Tags[parts[0]]["ref"]
		if !has {
			return fmt.Errorf("missing reference tag %s", ref)
		}
		parentType, has := engine.config.getEntityType(parentRef)
		if !has {
			return EntityNotRegisteredError{Name: tableSchema.Tags[parts[0]]["ref"]}
		}
		warmUpSubRefs[parentType] = append(warmUpSubRefs[parentType], parts[1:]...)

		for i := 0; i < l; i++ {
			var ref interface{}
			if many {
				ref = rows.Index(i).Elem().FieldByName(parts[0]).Interface()
			} else {
				ref = rows.FieldByName(parts[0]).Interface()
			}
			ids := make([]uint64, 0)
			oneRef, ok := ref.(*ReferenceOne)
			if ok {
				if oneRef.Id != 0 {
					ids = append(ids, oneRef.Id)
					if warmUpRefs[parentType] == nil {
						warmUpRefs[parentType] = make(map[uint64][]*ReferenceOne)
					}
					if warmUpRefs[parentType][oneRef.Id] == nil {
						warmUpRefs[parentType][oneRef.Id] = make([]*ReferenceOne, 0)
					}
					warmUpRefs[parentType][oneRef.Id] = append(warmUpRefs[parentType][oneRef.Id], oneRef)
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

		sub := reflect.New(reflect.SliceOf(reflect.PtrTo(t))).Elem()
		_, err := tryByIds(engine, ids, sub, warmUpSubRefs[t])
		if err != nil {
			return err
		}
		subLen := sub.Len()
		for i := 0; i < subLen; i++ {
			v := sub.Index(i)
			id := v.Elem().Field(1).Uint()
			entity := v.Interface()
			refs, has := warmUpRefs[t][id]
			if has {
				for _, ref := range refs {
					ref.Reference = entity
				}
			}
		}
	}
	return nil
}
