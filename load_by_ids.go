package orm

import (
	"fmt"
	"reflect"
	"strings"

	jsoniter "github.com/json-iterator/go"

	"github.com/juju/errors"
)

func tryByIDs(engine *Engine, ids []uint64, fillStruct bool, entities reflect.Value, references []string) (missing []uint64, schema *tableSchema) {
	missing = make([]uint64, 0)
	valOrigin := entities
	valOrigin.SetLen(0)
	valOrigin.SetCap(0)
	originalIDs := ids
	lenIDs := len(ids)
	if lenIDs == 0 {
		return
	}
	t, has, name := getEntityTypeForSlice(engine.registry, entities.Type())
	if !has {
		panic(fmt.Errorf("entity '%s' is not registered", name))
	}

	schema = getTableSchema(engine.registry, t)
	localCache, hasLocalCache := schema.GetLocalCache(engine)
	redisCache, hasRedis := schema.GetRedisCache(engine)

	if !hasLocalCache && engine.dataLoader != nil {
		data := engine.dataLoader.LoadAll(schema, ids)
		v := valOrigin
		for i, row := range data {
			if row == nil {
				missing = append(missing, ids[i])
			} else {
				val := reflect.New(schema.t)
				entity := val.Interface().(Entity)
				fillFromDBRow(ids[i], engine, row, entity, false)
				v = reflect.Append(v, val)
			}
		}
		valOrigin.Set(v)
		if len(references) > 0 && v.Len() > 0 {
			warmUpReferences(engine, schema, entities, references, true)
		}
		return
	}

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

	if !hasLocalCache && engine.hasRequestCache {
		hasLocalCache = true
		localCache = engine.GetLocalCache(requestCacheKey)
	}

	if hasLocalCache || hasRedis {
		if hasLocalCache {
			resultsLocalCache := localCache.MGet(cacheKeys...)
			cacheKeys = getKeysForNils(engine, schema, resultsLocalCache, keysMapping, results, false)
			localCacheKeys = cacheKeys
		}
		if hasRedis && len(cacheKeys) > 0 {
			resultsRedis := redisCache.MGet(cacheKeys...)
			cacheKeys = getKeysForNils(engine, schema, resultsRedis, keysMapping, results, true)
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

	valOrigin = entities
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

func getKeysForNils(engine *Engine, schema *tableSchema, rows map[string]interface{}, keysMapping map[string]uint64,
	results map[string]Entity, fromRedis bool) []string {
	keys := make([]string, 0)
	for k, v := range rows {
		if v == nil {
			keys = append(keys, k)
		} else {
			if v == "nil" {
				results[k] = nil
			} else if fromRedis {
				entity := reflect.New(schema.t).Interface().(Entity)
				var decoded []interface{}
				_ = jsoniter.ConfigFastest.Unmarshal([]byte(v.(string)), &decoded)
				convertDataFromJSON(schema.fields, 0, decoded)
				fillFromDBRow(keysMapping[k], engine, decoded, entity, false)
				results[k] = entity
			} else {
				entity := reflect.New(schema.t).Interface().(Entity)
				fillFromDBRow(keysMapping[k], engine, v.([]interface{}), entity, false)
				results[k] = entity
			}
		}
	}
	return keys
}

func warmUpReferences(engine *Engine, schema *tableSchema, rows reflect.Value, references []string, many bool) {
	dbMap := make(map[string]map[*tableSchema]map[string][]reflect.Value)
	var localMap map[string]map[string][]reflect.Value
	var redisMap map[string]map[string][]reflect.Value
	l := 1
	if many {
		l = rows.Len()
	}
	if references[0] == "*" {
		references = schema.refOne
	}
	var referencesNextLevel map[string]map[string][]Entity
	for _, ref := range references {
		refName := ref
		pos := strings.Index(refName, "/")
		if pos > 0 {
			if referencesNextLevel == nil {
				referencesNextLevel = make(map[string]map[string][]Entity)
			}
			nextRef := refName[pos+1:]
			refName = refName[0:pos]
			if referencesNextLevel[refName] == nil {
				referencesNextLevel[refName] = make(map[string][]Entity)
			}
			referencesNextLevel[refName][nextRef] = nil
		}
		_, has := schema.tags[refName]
		if !has {
			panic(errors.NotValidf("reference %s in %s", ref, schema.tableName))
		}
		parentRef, has := schema.tags[refName]["ref"]
		manyRef := false
		if !has {
			parentRef, has = schema.tags[refName]["refs"]
			manyRef = true
			if !has {
				panic(errors.NotValidf("reference tag %s", ref))
			}
		}
		parentSchema := engine.registry.tableSchemas[engine.registry.entities[parentRef]]
		hasLocalCache := parentSchema.hasLocalCache
		if !hasLocalCache && engine.hasRequestCache {
			hasLocalCache = true
		}
		if hasLocalCache && localMap == nil {
			localMap = make(map[string]map[string][]reflect.Value)
		}
		if parentSchema.hasRedisCache && redisMap == nil {
			redisMap = make(map[string]map[string][]reflect.Value)
		}

		for i := 0; i < l; i++ {
			var ref reflect.Value
			if many {
				ref = reflect.Indirect(rows.Index(i).Elem()).FieldByName(refName)
			} else {
				ref = rows.FieldByName(refName)
			}
			if !ref.IsValid() || ref.IsZero() {
				continue
			}
			if manyRef {
				length := ref.Len()
				for i := 0; i < length; i++ {
					fillRefMap(engine, referencesNextLevel, refName, ref.Index(i), parentSchema, dbMap, localMap, redisMap)
				}
			} else {
				fillRefMap(engine, referencesNextLevel, refName, ref, parentSchema, dbMap, localMap, redisMap)
			}
		}
	}
	for k, v := range localMap {
		if len(v) == 1 {
			var key string
			for k := range v {
				key = k
				break
			}
			fromCache, has := engine.GetLocalCache(k).Get(key)
			if has && fromCache != "nil" {
				data := fromCache.([]interface{})
				for _, r := range v[key] {
					fillFromDBRow(data[0].(uint64), engine, data, r.Interface().(Entity), false)
				}
				fillRef(key, localMap, redisMap, dbMap)
			}
		} else {
			keys := make([]string, len(v))
			i := 0
			for k := range v {
				keys[i] = k
				i++
			}
			for key, fromCache := range engine.GetLocalCache(k).MGet(keys...) {
				if fromCache != nil {
					data := fromCache.([]interface{})
					for _, r := range v[key] {
						fillFromDBRow(data[0].(uint64), engine, data, r.Interface().(Entity), false)
					}
					fillRef(key, localMap, redisMap, dbMap)
				}
			}
		}
	}
	for k, v := range redisMap {
		keys := make([]string, len(v))
		i := 0
		for k := range v {
			keys[i] = k
			i++
		}
		for key, fromCache := range engine.GetRedis(k).MGet(keys...) {
			if fromCache != nil {
				schema := v[key][0].Interface().(Entity).getORM().tableSchema
				decoded := make([]interface{}, len(schema.columnNames))
				_ = jsoniter.ConfigFastest.Unmarshal([]byte(fromCache.(string)), &decoded)
				convertDataFromJSON(schema.fields, 0, decoded)
				for _, r := range v[key] {
					fillFromDBRow(decoded[0].(uint64), engine, decoded, r.Interface().(Entity), false)
				}
				fillRef(key, nil, redisMap, dbMap)
			}
		}
	}
	for k, v := range dbMap {
		db := engine.GetMysql(k)
		for schema, v2 := range v {
			if len(v2) == 0 {
				continue
			}
			keys := make([]string, len(v2))
			q := make([]string, len(v2))
			i := 0
			for k2 := range v2 {
				keys[i] = k2[strings.Index(k2, ":")+1:]
				q[i] = keys[i]
				i++
			}
			query := "SELECT " + schema.fieldsQuery + " FROM `" + schema.tableName + "` WHERE `ID` IN (" + strings.Join(q, ",") + ")"
			results, def := db.Query(query)
			for results.Next() {
				pointers := prepareScan(schema)
				results.Scan(pointers...)
				convertScan(schema.fields, 0, pointers)
				id := pointers[0].(uint64)
				for _, r := range v2[schema.getCacheKey(id)] {
					fillFromDBRow(id, engine, pointers, r.Interface().(Entity), false)
				}
			}
			def()
		}
	}
	for pool, v := range redisMap {
		if len(v) == 0 {
			continue
		}
		values := make([]interface{}, 0)
		for cacheKey, refs := range v {
			e := refs[0].Interface().(Entity)
			if e.Loaded() {
				values = append(values, cacheKey, buildRedisValue(e))
			} else {
				values = append(values, cacheKey, "nil")
			}
		}
		engine.GetRedis(pool).MSet(values)
	}
	for pool, v := range localMap {
		if len(v) == 0 {
			continue
		}
		values := make([]interface{}, 0)
		for cacheKey, refs := range v {
			e := refs[0].Interface().(Entity)
			if e.Loaded() {
				values = append(values, cacheKey, buildLocalCacheValue(e))
			} else {
				values = append(values, cacheKey, "nil")
			}
		}
		engine.GetLocalCache(pool).MSet(values)
	}

	for _, v := range referencesNextLevel {
		for k, v2 := range v {
			l := len(v2)
			if l == 1 {
				warmUpReferences(engine, v2[0].getORM().tableSchema, reflect.ValueOf(v2[0]).Elem(), []string{k}, false)
			} else if l > 1 {
				warmUpReferences(engine, v2[0].getORM().tableSchema, reflect.ValueOf(v2), []string{k}, true)
			}
		}
	}
}

func fillRef(key string, localMap map[string]map[string][]reflect.Value,
	redisMap map[string]map[string][]reflect.Value, dbMap map[string]map[*tableSchema]map[string][]reflect.Value) {
	for _, p := range localMap {
		delete(p, key)
	}
	for _, p := range redisMap {
		delete(p, key)
	}
	for _, p := range dbMap {
		for _, p2 := range p {
			delete(p2, key)
		}
	}
}

func fillRefMap(engine *Engine, referencesNextLevel map[string]map[string][]Entity, refName string, v reflect.Value, parentSchema *tableSchema,
	dbMap map[string]map[*tableSchema]map[string][]reflect.Value,
	localMap map[string]map[string][]reflect.Value, redisMap map[string]map[string][]reflect.Value) {
	e := v.Interface().(Entity)
	if !e.Loaded() {
		id := e.GetID()
		if id > 0 {
			refSlice, has := referencesNextLevel[refName]
			if has {
				for k := range refSlice {
					refSlice[k] = append(refSlice[k], e)
				}
			}
			cacheKey := parentSchema.getCacheKey(id)
			if dbMap[parentSchema.mysqlPoolName] == nil {
				dbMap[parentSchema.mysqlPoolName] = make(map[*tableSchema]map[string][]reflect.Value)
			}
			if dbMap[parentSchema.mysqlPoolName][parentSchema] == nil {
				dbMap[parentSchema.mysqlPoolName][parentSchema] = make(map[string][]reflect.Value)
			}
			dbMap[parentSchema.mysqlPoolName][parentSchema][cacheKey] = append(dbMap[parentSchema.mysqlPoolName][parentSchema][cacheKey], v)
			hasLocalCache := parentSchema.hasLocalCache
			localCacheName := parentSchema.localCacheName
			if !hasLocalCache && engine.hasRequestCache {
				hasLocalCache = true
				localCacheName = requestCacheKey
			}
			if hasLocalCache {
				if localMap[localCacheName] == nil {
					localMap[localCacheName] = make(map[string][]reflect.Value)
				}
				localMap[localCacheName][cacheKey] = append(localMap[localCacheName][cacheKey], v)
			}
			if parentSchema.hasRedisCache {
				if redisMap[parentSchema.redisCacheName] == nil {
					redisMap[parentSchema.redisCacheName] = make(map[string][]reflect.Value)
				}
				redisMap[parentSchema.redisCacheName][cacheKey] = append(redisMap[parentSchema.redisCacheName][cacheKey], v)
			}
		}
	}
}
