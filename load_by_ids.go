package orm

import (
	"encoding/json"
	"reflect"
	"strings"

	"github.com/juju/errors"
)

func tryByIDs(engine *Engine, ids []uint64, entities reflect.Value, references []string) (missing []uint64) {
	originalIDs := ids
	lenIDs := len(ids)
	if lenIDs == 0 {
		entities.SetLen(0)
		return make([]uint64, 0)
	}
	t, has := getEntityTypeForSlice(engine.registry, entities.Type())
	if !has {
		panic(EntityNotRegisteredError{Name: entities.Type().String()})
	}

	schema := getTableSchema(engine.registry, t)
	localCache, hasLocalCache := schema.GetLocalCache(engine)
	redisCache, hasRedis := schema.GetRedisCache(engine)
	var localCacheKeys []string
	var redisCacheKeys []string
	results := make(map[string]Entity, lenIDs)
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
			cacheKeys = getKeysForNils(engine, schema.t, resultsLocalCache, keysMapping, results, false)
			localCacheKeys = cacheKeys
		}
		if hasRedis && len(cacheKeys) > 0 {
			resultsRedis := redisCache.MGet(cacheKeys...)
			cacheKeys = getKeysForNils(engine, schema.t, resultsRedis, keysMapping, results, true)
			redisCacheKeys = cacheKeys
		}
		ids = make([]uint64, len(cacheKeys))
		for k, v := range cacheKeys {
			ids[k] = keysMapping[v]
		}
	}
	l := len(ids)
	if l > 0 {
		_ = search(false, engine, NewWhere("`ID` IN ?", ids), NewPager(1, l), false, entities)
		for i := 0; i < entities.Len(); i++ {
			e := entities.Index(i).Interface().(Entity)
			results[schema.getCacheKey(e.GetID())] = e
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
				if val == nil {
					toSet = "nil"
				} else {
					toSet = buildLocalCacheValue(val)
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
				if val == nil {
					toSet = "nil"
				} else {
					toSet = buildRedisValue(val)
				}
				pairs[i+1] = toSet
				i += 2
			}
			redisCache.MSet(pairs...)
		}
	}

	missing = make([]uint64, 0)
	valOrigin := entities
	valOrigin.SetLen(0)
	valOrigin.SetCap(0)
	v := valOrigin
	for _, id := range originalIDs {
		val := results[keysReversed[id]]
		if val == nil {
			missing = append(missing, id)
		} else {
			v = reflect.Append(v, reflect.ValueOf(val))
		}
	}
	valOrigin.Set(v)
	if len(references) > 0 && v.Len() > 0 {
		warmUpReferences(engine, schema, entities, references, true)
	}
	return
}

func getKeysForNils(engine *Engine, entityType reflect.Type, rows map[string]interface{}, keysMapping map[string]uint64,
	results map[string]Entity, fromRedis bool) []string {
	keys := make([]string, 0)
	for k, v := range rows {
		if v == nil {
			keys = append(keys, k)
		} else {
			if v == "nil" {
				results[k] = nil
			} else if fromRedis {
				entity := reflect.New(entityType).Interface().(Entity)
				var decoded []string
				_ = json.Unmarshal([]byte(v.(string)), &decoded)
				fillFromDBRow(keysMapping[k], engine, decoded, entity)
				results[k] = entity
			} else {
				entity := reflect.New(entityType).Interface().(Entity)
				fillFromDBRow(keysMapping[k], engine, v.([]string), entity)
				results[k] = entity
			}
		}
	}
	return keys
}

func warmUpReferences(engine *Engine, tableSchema *tableSchema, rows reflect.Value, references []string, many bool) {
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
			panic(errors.NotValidf("reference %s in %s", ref, tableSchema.tableName))
		}
		parentRef, has := tableSchema.tags[parts[0]]["ref"]
		if !has {
			panic(errors.NotValidf("reference tag %s", ref))
		}
		parentType, has := engine.registry.entities[parentRef]
		if !has {
			panic(EntityNotRegisteredError{Name: tableSchema.tags[parts[0]]["ref"]})
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
			refEntity := ref.Interface().(Entity)
			refID := refEntity.GetID()
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
		_ = tryByIDs(engine, ids, sub, warmUpSubRefs[t])
		subLen := sub.Len()
		for i := 0; i < subLen; i++ {
			v := sub.Index(i).Interface().(Entity)
			id := v.GetID()
			refs, has := warmUpRefs[t][id]
			if has {
				for _, ref := range refs {
					ref.Set(v.getORM().attributes.value)
				}
			}
		}
	}
}
