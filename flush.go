package orm

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type DuplicatedKeyError struct {
	Message string
	Index   string
}

func (err *DuplicatedKeyError) Error() string {
	return err.Message
}

func Flush(entities ...interface{}) error {
	return flush(false, entities...)
}

func FlushLazy(entities ...interface{}) error {
	return flush(true, entities...)
}

func flush(lazy bool, entities ...interface{}) error {
	insertKeys := make(map[reflect.Type][]string)
	insertValues := make(map[reflect.Type]string)
	insertArguments := make(map[reflect.Type][]interface{})
	insertBinds := make(map[reflect.Type][]map[string]interface{})
	insertReflectValues := make(map[reflect.Type][]reflect.Value)
	deleteBinds := make(map[reflect.Type]map[uint64]map[string]interface{})
	totalInsert := make(map[reflect.Type]int)
	localCacheSets := make(map[string]map[string][]interface{})
	localCacheDeletes := make(map[string]map[string]map[string]bool)
	redisKeysToDelete := make(map[string]map[string]map[string]bool)
	dirtyQueues := make(map[string][]interface{})
	lazyMap := make(map[string]interface{})

	for _, entity := range entities {
		v := reflect.ValueOf(entity)
		value := reflect.Indirect(v)
		orm, err := initIfNeeded(v)
		if err != nil {
			return err
		}
		isDirty, bind, err := orm.isDirty(value)
		if err != nil {
			return err
		}
		if !isDirty {
			continue
		}
		bindLength := len(bind)

		t := value.Type()
		if len(orm.dBData) == 0 {
			values := make([]interface{}, bindLength)
			valuesKeys := make([]string, bindLength)
			if insertKeys[t] == nil {
				fields := make([]string, bindLength)
				i := 0
				for key := range bind {
					fields[i] = key
					i++
				}
				insertKeys[t] = fields
			}
			for index, key := range insertKeys[t] {
				value := bind[key]
				values[index] = value
				valuesKeys[index] = "?"
			}
			_, has := insertArguments[t]
			if !has {
				insertArguments[t] = make([]interface{}, 0)
				insertReflectValues[t] = make([]reflect.Value, 0)
				insertBinds[t] = make([]map[string]interface{}, 0)
				insertValues[t] = fmt.Sprintf("(%s)", strings.Join(valuesKeys, ","))
			}
			insertArguments[t] = append(insertArguments[t], values...)
			insertReflectValues[t] = append(insertReflectValues[t], value)
			insertBinds[t] = append(insertBinds[t], bind)
			totalInsert[t]++
		} else {
			values := make([]interface{}, bindLength+1)
			currentId := value.Field(1).Uint()
			if orm.dBData["_delete"] == true {
				if deleteBinds[t] == nil {
					deleteBinds[t] = make(map[uint64]map[string]interface{})
					deleteBinds[t][currentId] = orm.dBData
				}
			} else {
				fields := make([]string, bindLength)
				i := 0
				for key, value := range bind {
					fields[i] = fmt.Sprintf("`%s` = ?", key)
					values[i] = value
					i++
				}
				schema := getTableSchema(t)
				sql := schema.GetMysql().databaseInterface.GetUpdateQuery(schema.TableName, fields)
				db := schema.GetMysql()
				values[i] = currentId
				if lazy && db.transaction == nil {
					fillLazyQuery(lazyMap, db.code, sql, values)
				} else {
					_, err := schema.GetMysql().Exec(sql, values...)
					if err != nil {
						return convertToDuplicateKeyError(schema, err)
					}
				}
				old := make(map[string]interface{}, len(orm.dBData))
				for k, v := range orm.dBData {
					old[k] = v
				}
				injectBind(value, bind)
				localCache := schema.GetLocalCache()
				redisCache := schema.GetRedisCacheContainer()

				if localCache != nil {
					db := schema.GetMysql()
					addLocalCacheSet(localCacheSets, db.code, localCache.code, schema.getCacheKey(currentId), buildLocalCacheValue(value.Interface(), schema))
					keys, err := getCacheQueriesKeys(schema, bind, orm.dBData, false)
					if err != nil {
						return err
					}
					addCacheDeletes(localCacheDeletes, db.code, localCache.code, keys...)
					keys, err = getCacheQueriesKeys(schema, bind, old, false)
					if err != nil {
						return err
					}
					addCacheDeletes(localCacheDeletes, db.code, localCache.code, keys...)
				}
				if redisCache != nil {
					db := schema.GetMysql()
					addCacheDeletes(redisKeysToDelete, db.code, redisCache.code, schema.getCacheKey(currentId))
					keys, err := getCacheQueriesKeys(schema, bind, orm.dBData, false)
					if err != nil {
						return err
					}
					addCacheDeletes(redisKeysToDelete, db.code, redisCache.code, keys...)
					keys, err = getCacheQueriesKeys(schema, bind, old, false)
					if err != nil {
						return err
					}
					addCacheDeletes(redisKeysToDelete, db.code, redisCache.code, keys...)
				}
				addDirtyQueues(dirtyQueues, bind, schema, currentId, "u")
			}
		}
	}
	for typeOf, values := range insertKeys {
		schema := getTableSchema(typeOf)
		finalValues := make([]string, len(values))
		for key, val := range values {
			finalValues[key] = fmt.Sprintf("`%s`", val)
		}
		sql := schema.GetMysql().databaseInterface.GetInsertQuery(schema.TableName, finalValues, insertValues[typeOf])
		for i := 1; i < totalInsert[typeOf]; i++ {
			sql += "," + insertValues[typeOf]
		}
		id := uint64(0)
		db := schema.GetMysql()
		if lazy {
			fillLazyQuery(lazyMap, db.code, sql, insertArguments[typeOf])
		} else {
			res, err := db.Exec(sql, insertArguments[typeOf]...)
			if err != nil {
				return convertToDuplicateKeyError(schema, err)
			}
			insertId, err := res.LastInsertId()
			if err != nil {
				return err
			}
			id = uint64(insertId)
		}
		localCache := schema.GetLocalCache()
		redisCache := schema.GetRedisCacheContainer()
		for key, value := range insertReflectValues[typeOf] {
			bind := insertBinds[typeOf][key]
			injectBind(value, bind)
			value.Field(1).SetUint(id)
			if localCache != nil {
				if !lazy {
					addLocalCacheSet(localCacheSets, db.code, localCache.code, schema.getCacheKey(id), buildLocalCacheValue(value.Interface(), schema))
				}
				keys, err := getCacheQueriesKeys(schema, bind, bind, true)
				if err != nil {
					return err
				}
				addCacheDeletes(localCacheDeletes, db.code, localCache.code, keys...)
			}
			if redisCache != nil {
				if !lazy {
					addCacheDeletes(redisKeysToDelete, db.code, redisCache.code, schema.getCacheKey(id))
				}
				keys, err := getCacheQueriesKeys(schema, bind, bind, true)
				if err != nil {
					return err
				}
				addCacheDeletes(redisKeysToDelete, db.code, redisCache.code, keys...)
			}
			addDirtyQueues(dirtyQueues, bind, schema, id, "i")
			id++
		}
	}
	for typeOf, deleteBinds := range deleteBinds {
		schema := getTableSchema(typeOf)
		ids := make([]interface{}, len(deleteBinds))
		i := 0
		for id := range deleteBinds {
			ids[i] = id
			i++
		}
		sql := schema.GetMysql().databaseInterface.GetDeleteQuery(schema.TableName, ids)
		db := schema.GetMysql()
		if lazy && db.transaction == nil {
			fillLazyQuery(lazyMap, db.code, sql, ids)
		} else {
			_, err := db.Exec(sql, ids...)
			if err != nil {
				return err
			}
		}

		localCache := schema.GetLocalCache()
		redisCache := schema.GetRedisCacheContainer()
		if localCache != nil {
			for id, bind := range deleteBinds {
				addLocalCacheSet(localCacheSets, db.code, localCache.code, schema.getCacheKey(id), "nil")
				keys, err := getCacheQueriesKeys(schema, bind, bind, true)
				if err != nil {
					return err
				}
				addCacheDeletes(localCacheDeletes, db.code, localCache.code, keys...)
			}
		}
		if schema.redisCacheName != "" {
			for id, bind := range deleteBinds {
				addCacheDeletes(redisKeysToDelete, db.code, redisCache.code, schema.getCacheKey(id))
				keys, err := getCacheQueriesKeys(schema, bind, bind, true)
				if err != nil {
					return err
				}
				addCacheDeletes(redisKeysToDelete, db.code, redisCache.code, keys...)
			}
		}
		for id, bind := range deleteBinds {
			addDirtyQueues(dirtyQueues, bind, schema, id, "d")
		}
	}

	for dbCode, values := range localCacheSets {
		db := GetMysql(dbCode)
		for cacheCode, keys := range values {
			cache := GetLocalCache(cacheCode)
			if db.transaction == nil {
				cache.MSet(keys...)
			} else {
				if db.afterCommitLocalCacheSets == nil {
					db.afterCommitLocalCacheSets = make(map[string][]interface{})
				}
				db.afterCommitLocalCacheSets[cacheCode] = append(db.afterCommitLocalCacheSets[cacheCode], keys...)
			}
		}
	}
	for dbCode, values := range localCacheDeletes {
		db := GetMysql(dbCode)
		for cacheCode, allKeys := range values {
			cache := GetLocalCache(cacheCode)
			keys := make([]string, len(allKeys))
			i := 0
			for key := range allKeys {
				keys[i] = key
				i++
			}
			if db.transaction == nil {
				if lazy {
					deletesLocalCache := lazyMap["cl"]
					if deletesLocalCache == nil {
						deletesLocalCache = make(map[string][]string)
						lazyMap["cl"] = deletesLocalCache
					}
					deletesLocalCache.(map[string][]string)[cacheCode] = keys
				} else {
					cache.Remove(keys...)
				}
			} else {
				if db.afterCommitLocalCacheDeletes == nil {
					db.afterCommitLocalCacheDeletes = make(map[string][]string)
				}
				db.afterCommitLocalCacheDeletes[cacheCode] = append(db.afterCommitLocalCacheDeletes[cacheCode], keys...)
			}
		}
	}
	for dbCode, values := range redisKeysToDelete {
		db := GetMysql(dbCode)
		for cacheCode, allKeys := range values {
			cache := GetRedis(cacheCode)
			keys := make([]string, len(allKeys))
			i := 0
			for key := range allKeys {
				keys[i] = key
				i++
			}
			if db.transaction == nil {
				if lazy {
					deletesRedisCache := lazyMap["cr"]
					if deletesRedisCache == nil {
						deletesRedisCache = make(map[string][]string)
						lazyMap["cr"] = deletesRedisCache
					}
					deletesRedisCache.(map[string][]string)[cacheCode] = keys
				} else {
					err := cache.Del(keys...)
					if err != nil {
						return err
					}
				}
			} else {
				if db.afterCommitRedisCacheDeletes == nil {
					db.afterCommitRedisCacheDeletes = make(map[string][]string)
				}
				db.afterCommitRedisCacheDeletes[cacheCode] = append(db.afterCommitRedisCacheDeletes[cacheCode], keys...)
			}
		}
	}
	if len(lazyMap) > 0 {
		v, err := serializeForLazyQueue(lazyMap)
		if err != nil {
			return err
		}
		_, err = getRedisForQueue("default").LPush("lazy_queue", v)
		if err != nil {
			return err
		}
	}

	for k, v := range dirtyQueues {
		redisCode, has := dirtyQueuesCodes[k]
		if !has {
			return fmt.Errorf("unregistered lazy queue %s", k)
		}
		_, err := GetRedis(redisCode).SAdd(k, v...)
		if err != nil {
			return err
		}
	}
	return handleLazyReferences(entities...)
}

func handleLazyReferences(entities ...interface{}) error {
	toFlush := make([]interface{}, 0)
	for _, entity := range entities {
		dirty := false
		value := reflect.Indirect(reflect.ValueOf(entity))
		schema := getTableSchema(value.Type())
		for _, columnName := range schema.refOne {
			refOne := value.FieldByName(columnName).Interface().(*ReferenceOne)
			if refOne.Id == 0 && refOne.Reference != nil {
				refId := reflect.Indirect(reflect.ValueOf(refOne.Reference)).Field(1).Uint()
				if refId > 0 {
					refOne.Id = refId
					dirty = true
				}
			}
		}
		if dirty {
			toFlush = append(toFlush, entity)
		}
	}
	if len(toFlush) > 0 {
		err := flush(false, toFlush...)
		if err != nil {
			return err
		}
	}

	return nil
}

func serializeForLazyQueue(lazyMap map[string]interface{}) (string, error) {
	encoded, err := json.Marshal(lazyMap)
	if err != nil {
		return "", err
	}
	return string(encoded), nil

}
func injectBind(value reflect.Value, bind map[string]interface{}) {
	oldFields := value.Field(0).Interface().(*ORM)
	if oldFields.dBData == nil {
		oldFields.dBData = make(map[string]interface{})
	}
	for key, value := range bind {
		oldFields.dBData[key] = value
	}
	value.Field(0).Set(reflect.ValueOf(oldFields))
}

func createBind(tableSchema *TableSchema, t reflect.Type, value reflect.Value, oldData map[string]interface{}, prefix string) (bind map[string]interface{}, err error) {
	bind = make(map[string]interface{})
	var hasOld = len(oldData) > 0
	for i := 0; i < t.NumField(); i++ {
		fieldType := t.Field(i)
		name := prefix + fieldType.Name
		if prefix == "" && i <= 1 {
			continue
		}
		old, _ := oldData[name]
		field := value.Field(i)
		attributes := tableSchema.Tags[name]
		_, has := attributes["ignore"]
		if has {
			continue
		}
		required, hasRequired := attributes["required"]
		isRequired := hasRequired && required == "true"
		switch field.Type().String() {
		case "uint", "uint8", "uint16", "uint32", "uint64":
			val := field.Uint()
			valString := strconv.FormatUint(val, 10)
			year, _ := attributes["year"]
			if year == "true" {
				valString = fmt.Sprintf("%04d", val)
				if hasOld && (old == valString || old == nil && valString == "0000") {
					continue
				}
				if !isRequired && val == 0 {
					bind[name] = nil
				} else {
					bind[name] = valString
				}
				continue
			}
			if hasOld && old == valString {
				continue
			}
			bind[name] = valString
		case "int", "int8", "int16", "int32", "int64":
			val := strconv.FormatInt(field.Int(), 10)
			if hasOld && old == val {
				continue
			}
			bind[name] = val
		case "string":
			value := field.String()
			if hasOld && (old == value || (old == nil && value == "")) {
				continue
			}
			if value == "" {
				if isRequired {
					bind[name] = ""
				} else {
					bind[name] = nil
				}
			} else {
				bind[name] = value
			}
		case "[]uint8":
			value := field.Bytes()
			valueAsString := string(value)
			if hasOld && (old == valueAsString || (old == nil && valueAsString == "")) {
				continue
			}
			if valueAsString == "" {
				bind[name] = nil
			} else {
				bind[name] = value
			}
		case "bool":
			value := "0"
			if field.Bool() {
				value = "1"
			}
			if hasOld && old == value {
				continue
			}
			bind[name] = value
		case "float32", "float64":
			val := field.Float()
			precision := 8
			bitSize := 32
			if field.Type().String() == "float64" {
				bitSize = 64
				precision = 16
			}
			fieldAttributes := tableSchema.Tags[name]
			precisionAttribute, has := fieldAttributes["precision"]
			if has {
				userPrecision, err := strconv.Atoi(precisionAttribute)
				if err != nil {
					return nil, err
				}
				precision = userPrecision
			}
			valString := strconv.FormatFloat(val, 'g', precision, bitSize)
			decimal, has := attributes["decimal"]
			if has {
				decimalArgs := strings.Split(decimal, ",")
				valString = fmt.Sprintf("%."+decimalArgs[1]+"f", val)
			}
			if hasOld && old == valString {
				continue
			}
			bind[name] = valString
		case "[]string":
			value := field.Interface().([]string)
			var valueAsString string
			if value != nil {
				valueAsString = strings.Join(value, ",")
			}
			if hasOld && old == valueAsString {
				continue
			}
			bind[name] = valueAsString
		case "[]uint64":
			value := field.Interface()
			var valueAsString string
			if value != nil {
				valueAsString = fmt.Sprintf("%v", value)
				valueAsString = strings.Trim(valueAsString, "[]")
			}
			if hasOld && old == valueAsString {
				continue
			}
			if valueAsString == "" {
				bind[name] = nil
			} else {
				bind[name] = valueAsString
			}
		case "*orm.ReferenceOne":
			valueAsString := strconv.FormatUint(field.Interface().(*ReferenceOne).Id, 10)
			if hasOld && (old == valueAsString || (old == nil && valueAsString == "0")) {
				continue
			}
			if valueAsString == "0" {
				bind[name] = nil
			} else {
				bind[name] = valueAsString
			}
		case "*orm.CachedQuery":
			continue
		case "time.Time":
			value := field.Interface().(time.Time)
			layout := "2006-01-02"
			layoutEmpty := "0001-01-01"
			fieldAttributes := tableSchema.Tags[name]
			timeAttribute, _ := fieldAttributes["time"]
			var valueAsString string
			if timeAttribute == "true" {
				layoutEmpty += " 00:00:00"
				if value.Year() == 1 {
					valueAsString = "0001-01-01 00:00:00"
				} else {
					layout += " 15:04:05"
				}
			} else if value.Year() == 1 {
				valueAsString = "0001-01-01"
			}
			if valueAsString == "" {
				valueAsString = value.Format(layout)
			}
			if isRequired {
				if hasOld && old == valueAsString {
					continue
				}
				bind[name] = valueAsString
				continue
			}
			if hasOld && (old == valueAsString || old == nil && valueAsString == layoutEmpty) {
				continue
			}
			if valueAsString == layoutEmpty {
				bind[name] = nil
			} else {
				bind[name] = valueAsString
			}
		case "interface {}":
			value := field.Interface()
			var valString string
			if value != nil && value != "" {
				encoded, err := json.Marshal(value)
				if err != nil {
					return nil, fmt.Errorf("invalid json to encode: %v", value)
				}
				asString := string(encoded)
				if asString != "" {
					valString = asString
				}
			}
			if hasOld && old == valString {
				continue
			}
			bind[name] = valString
		default:
			if field.Kind().String() == "struct" {
				subBind, err := createBind(tableSchema, field.Type(), reflect.ValueOf(field.Interface()), oldData, fieldType.Name)
				if err != nil {
					return nil, err
				}
				for key, value := range subBind {
					bind[key] = value
				}
				continue
			}
			return nil, fmt.Errorf("unsoported field type: %s", field.Type().String())
		}
	}
	return
}

func getCacheQueriesKeys(schema *TableSchema, bind map[string]interface{}, data map[string]interface{}, addedDeleted bool) (keys []string, err error) {
	keys = make([]string, 0)
	for indexName, definition := range schema.cachedIndexes {
		if addedDeleted && len(definition.Fields) == 0 {
			keys = append(keys, schema.getCacheKeySearch(indexName))
		}
		for _, trackedField := range definition.Fields {
			_, has := bind[trackedField]
			if has {
				attributes := make([]interface{}, len(definition.Fields))
				i := 0
				for _, trackedFieldSub := range definition.Fields {
					attributes[i], has = data[trackedFieldSub]
					if !has {
						return nil, fmt.Errorf("missing field %s in index", trackedFieldSub)
					}
					i++
				}
				keys = append(keys, schema.getCacheKeySearch(indexName, attributes...))
				break
			}
		}
	}
	return
}

func addLocalCacheSet(localCacheSets map[string]map[string][]interface{}, dbCode string, cacheCode string, keys ...interface{}) {
	if localCacheSets[dbCode] == nil {
		localCacheSets[dbCode] = make(map[string][]interface{})
	}
	localCacheSets[dbCode][cacheCode] = append(localCacheSets[dbCode][cacheCode], keys...)
}

func addCacheDeletes(cacheDeletes map[string]map[string]map[string]bool, dbCode string, cacheCode string, keys ...string) {
	if len(keys) == 0 {
		return
	}
	if cacheDeletes[dbCode] == nil {
		cacheDeletes[dbCode] = make(map[string]map[string]bool)
	}
	if cacheDeletes[dbCode][cacheCode] == nil {
		cacheDeletes[dbCode][cacheCode] = make(map[string]bool)
	}
	for _, key := range keys {
		cacheDeletes[dbCode][cacheCode][key] = true
	}
}

func addDirtyQueues(keys map[string][]interface{}, bind map[string]interface{}, schema *TableSchema, id uint64, action string) {
	results := make(map[string]interface{})
	key := createDirtyQueueMember(schema.t.String()+":"+action, id)
	for column, tags := range schema.Tags {
		queues, has := tags["dirty"]
		if !has {
			continue
		}
		isDirty := column == "Orm"
		if !isDirty {
			_, isDirty = bind[column]
		}
		if !isDirty {
			continue
		}
		queueNames := strings.Split(queues, ",")
		for _, queueName := range queueNames {
			_, has = results[queueName]
			if has {
				continue
			}
			results[queueName] = key
		}
	}
	for k, v := range results {
		keys[k] = append(keys[k], v)
	}
}

func fillLazyQuery(lazyMap map[string]interface{}, dbCode string, sql string, values []interface{}) {
	updatesMap := lazyMap["q"]
	if updatesMap == nil {
		updatesMap = make([]interface{}, 0)
		lazyMap["q"] = updatesMap
	}
	lazyValue := make([]interface{}, 3)
	lazyValue[0] = dbCode
	lazyValue[1] = sql
	lazyValue[2] = values
	lazyMap["q"] = append(updatesMap.([]interface{}), lazyValue)
}

func convertToDuplicateKeyError(schema *TableSchema, err error) error {
	return schema.GetMysql().databaseInterface.ConvertToDuplicateKeyError(err)
}
