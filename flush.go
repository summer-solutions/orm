package orm

import (
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func IsDirty(entity interface{}) (is bool, bind map[string]interface{}) {
	v := reflect.ValueOf(entity)
	value := reflect.Indirect(v)
	t := value.Type()
	ormField := value.Field(0).Interface().(ORM)
	if ormField.DBData["_delete"] == true {
		return true, nil
	}
	bind = createBind(GetTableSchema(t.String()), t, value, ormField.DBData, "")
	is = value.Field(1).Uint() == 0 || len(bind) > 0
	return
}

func Flush(entities ...interface{}) (err error) {
	return flush(false, false, entities...)
}

func FlushLazy(entities ...interface{}) (err error) {
	return flush(true, false, entities...)
}

func flush(lazy bool, dirty bool, entities ...interface{}) (err error) {
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
	lazyMap := make(map[string]interface{})
	contextCache := getContextCache()

	for _, entity := range entities {
		isDirty, bind := IsDirty(entity)
		if !isDirty {
			continue
		}
		bindLength := len(bind)
		value := reflect.Indirect(reflect.ValueOf(entity))
		orm := value.Field(0).Interface().(ORM)

		t := value.Type()
		if orm.DBData == nil {
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
			if orm.DBData["_delete"] == true {
				if deleteBinds[t] == nil {
					deleteBinds[t] = make(map[uint64]map[string]interface{})
					deleteBinds[t][currentId] = orm.DBData
				}
			} else {
				fields := make([]string, bindLength)
				i := 0
				for key, value := range bind {
					fields[i] = fmt.Sprintf("`%s` = ?", key)
					values[i] = value
					i++
				}
				schema := GetTableSchema(t.String())
				sql := fmt.Sprintf("UPDATE %s SET %s WHERE `Id` = ?", schema.TableName, strings.Join(fields, ","))
				db := schema.GetMysqlDB()
				values[i] = currentId
				if lazy && db.transaction == nil {
					fillLazyQuery(lazyMap, db.code, sql, values)
				} else {
					_, err := schema.GetMysqlDB().Exec(sql, values...)
					if err != nil {
						return err
					}
				}
				old := make(map[string]interface{}, len(orm.DBData))
				for k, v := range orm.DBData {
					old[k] = v
				}
				injectBind(value, bind)
				localCache := schema.GetLocalCacheContainer()
				contextCache := getContextCache()
				if localCache == nil && contextCache != nil {
					localCache = contextCache
				}
				redisCache := schema.GetRedisCacheContainer()

				if localCache != nil {
					db := schema.GetMysqlDB()
					addLocalCacheSet(localCacheSets, db.code, localCache.code, schema.getCacheKey(currentId), value.Interface())
					addCacheDeletes(localCacheDeletes, db.code, localCache.code, getCacheQueriesKeys(schema, bind, orm.DBData, false)...)
					addCacheDeletes(localCacheDeletes, db.code, localCache.code, getCacheQueriesKeys(schema, bind, old, false)...)
				}
				if redisCache != nil {
					db := schema.GetMysqlDB()
					if !dirty {
						addCacheDeletes(redisKeysToDelete, db.code, redisCache.code, schema.getCacheKey(currentId))
					}
					addCacheDeletes(redisKeysToDelete, db.code, redisCache.code, getCacheQueriesKeys(schema, bind, orm.DBData, false)...)
					addCacheDeletes(redisKeysToDelete, db.code, redisCache.code, getCacheQueriesKeys(schema, bind, old, false)...)
				}
			}
		}
	}
	for typeOf, values := range insertKeys {
		schema := GetTableSchema(typeOf.String())
		finalValues := make([]string, len(values))
		for key, val := range values {
			finalValues[key] = fmt.Sprintf("`%s`", val)
		}
		sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES %s", schema.TableName, strings.Join(finalValues, ","), insertValues[typeOf])
		for i := 1; i < totalInsert[typeOf]; i++ {
			sql += "," + insertValues[typeOf]
		}
		id := uint64(0)
		db := schema.GetMysqlDB()
		if lazy {
			fillLazyQuery(lazyMap, db.code, sql, insertArguments[typeOf])
		} else {
			res, err := db.Exec(sql, insertArguments[typeOf]...)
			if err != nil {
				return err
			}
			insertId, err := res.LastInsertId()
			if err != nil {
				return err
			}
			id = uint64(insertId)
		}
		localCache := schema.GetLocalCacheContainer()
		if localCache == nil && contextCache != nil {
			localCache = contextCache
		}
		redisCache := schema.GetRedisCacheContainer()
		for key, value := range insertReflectValues[typeOf] {
			bind := insertBinds[typeOf][key]
			injectBind(value, bind)
			value.Field(1).SetUint(id)
			if localCache != nil {
				if !lazy {
					addLocalCacheSet(localCacheSets, db.code, localCache.code, schema.getCacheKey(id), value.Interface())
				}
				addCacheDeletes(localCacheDeletes, db.code, localCache.code, getCacheQueriesKeys(schema, bind, bind, true)...)
			}
			if redisCache != nil {
				if !lazy {
					addCacheDeletes(redisKeysToDelete, db.code, redisCache.code, schema.getCacheKey(id))
				}
				addCacheDeletes(redisKeysToDelete, db.code, redisCache.code, getCacheQueriesKeys(schema, bind, bind, true)...)
			}
			id++
		}
	}
	for typeOf, deleteBinds := range deleteBinds {
		schema := GetTableSchema(typeOf.String())
		ids := make([]uint64, len(deleteBinds))
		i := 0
		for id := range deleteBinds {
			ids[i] = id
			i++
		}
		where := NewWhere("`Id` IN ?", ids)
		sql := fmt.Sprintf("DELETE FROM `%s` WHERE %s", schema.TableName, where)
		db := schema.GetMysqlDB()
		if lazy && db.transaction == nil {
			fillLazyQuery(lazyMap, db.code, sql, where.GetParameters())
		} else {
			_, err = db.Exec(sql, where.GetParameters()...)
			if err != nil {
				return err
			}
		}

		localCache := schema.GetLocalCacheContainer()
		if localCache == nil && contextCache != nil {
			localCache = contextCache
		}
		redisCache := schema.GetRedisCacheContainer()
		if localCache != nil {
			for id, bind := range deleteBinds {
				addLocalCacheSet(localCacheSets, db.code, localCache.code, schema.getCacheKey(id), nil)
				addCacheDeletes(localCacheDeletes, db.code, localCache.code, getCacheQueriesKeys(schema, bind, bind, true)...)
			}
		}
		if schema.redisCacheName != "" {
			for id, bind := range deleteBinds {
				addCacheDeletes(redisKeysToDelete, db.code, redisCache.code, schema.getCacheKey(id))
				addCacheDeletes(redisKeysToDelete, db.code, redisCache.code, getCacheQueriesKeys(schema, bind, bind, true)...)
			}
		}
	}

	for dbCode, values := range localCacheSets {
		db := GetMysqlDB(dbCode)
		for cacheCode, keys := range values {
			cache := GetLocalCacheContainer(cacheCode)
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
		db := GetMysqlDB(dbCode)
		for cacheCode, allKeys := range values {
			cache := GetLocalCacheContainer(cacheCode)
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
					cache.RemoveMany(keys...)
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
		db := GetMysqlDB(dbCode)
		for cacheCode, allKeys := range values {
			cache := GetRedisCache(cacheCode)
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
					err = cache.Del(keys...)
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
		GetRedisCache(queueRedisName).LPush("lazy_queue", serializeForLazyQueue(lazyMap))
	}
	return
}

func serializeForLazyQueue(lazyMap map[string]interface{}) string {
	encoded, err := json.Marshal(lazyMap)
	if err != nil {
		panic(err.Error())
	}
	return string(encoded)

}
func injectBind(value reflect.Value, bind map[string]interface{}) {
	oldFields := value.Field(0).Interface().(ORM)
	if oldFields.DBData == nil {
		oldFields.DBData = make(map[string]interface{})
	}
	for key, value := range bind {
		oldFields.DBData[key] = value
	}
	value.Field(0).Set(reflect.ValueOf(oldFields))
}

func createBind(tableSchema *TableSchema, t reflect.Type, value reflect.Value, oldData map[string]interface{}, prefix string) (bind map[string]interface{}) {
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
		attributes := tableSchema.tags[name]
		switch field.Type().String() {
		case "uint", "uint8", "uint16", "uint32", "uint64":
			val := field.Uint()
			valString := strconv.FormatUint(val, 10)
			year, _ := attributes["year"]
			if year == "true" {
				valString = fmt.Sprintf("%04d", val)
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
			fieldAttributes := tableSchema.tags[name]
			precisionAttribute, has := fieldAttributes["precision"]
			if has {
				userPrecision, err := strconv.Atoi(precisionAttribute)
				if err != nil {
					panic(err.Error())
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
		case "time.Time":
			value := field.Interface().(time.Time)
			layout := "2006-01-02"
			fieldAttributes := tableSchema.tags[name]
			timeAttribute, _ := fieldAttributes["time"]
			var valueAsString string
			if timeAttribute == "true" {
				if value.Year() == 1 {
					valueAsString = "0000-00-00 00:00:00"
				} else {
					layout += " 15:04:05"
				}
			} else if value.Year() == 1 {
				valueAsString = "0000-00-00"
			}
			if valueAsString == "" {
				valueAsString = value.Format(layout)
			}
			if hasOld && old == valueAsString {
				continue
			}
			bind[name] = valueAsString
		case "interface {}":
			value := field.Interface()
			var valString string
			if value != nil && value != "" {
				encoded, err := json.Marshal(value)
				if err != nil {
					panic(fmt.Errorf("invalid json to encode: %v", value))
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
				subBind := createBind(tableSchema, field.Type(), reflect.ValueOf(field.Interface()), oldData, fieldType.Name)
				for key, value := range subBind {
					bind[key] = value
				}
				continue
			}
			panic(fmt.Errorf("unsoported field type: %s", field.Type().String()))
		}
	}
	return
}

func getCacheQueriesKeys(schema *TableSchema, bind map[string]interface{}, data map[string]interface{}, addedDeleted bool) (keys []string) {
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
				for _, trackedField = range definition.Fields {

					attributes[i] = data[trackedField]
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
