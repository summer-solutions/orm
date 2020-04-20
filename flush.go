package orm

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

type DuplicatedKeyError struct {
	Message string
	Index   string
}

func (err *DuplicatedKeyError) Error() string {
	return err.Message
}

type ForeignKeyError struct {
	Message    string
	Constraint string
}

func (err *ForeignKeyError) Error() string {
	return err.Message
}

func flush(engine *Engine, lazy bool, entities ...reflect.Value) error {
	insertKeys := make(map[reflect.Type][]string)
	insertValues := make(map[reflect.Type]string)
	insertArguments := make(map[reflect.Type][]interface{})
	insertBinds := make(map[reflect.Type][]map[string]interface{})
	insertReflectValues := make(map[reflect.Type][]reflect.Value)
	deleteBinds := make(map[reflect.Type]map[uint64]map[string]interface{})
	totalInsert := make(map[reflect.Type]int)
	localCacheSets := make(map[string]map[string][]interface{})
	localCacheDeletes := make(map[string]map[string]bool)
	redisKeysToDelete := make(map[string]map[string]bool)
	dirtyQueues := make(map[string][]*DirtyQueueValue)
	logQueues := make(map[string][]*LogQueueValue)
	lazyMap := make(map[string]interface{})

	for _, v := range entities {
		if !v.IsValid() {
			return fmt.Errorf("unregistered struct. run engine.RegisterNewEntity(entity) before entity.Flush()")
		}
		value := reflect.Indirect(v)
		validate, is := v.Interface().(ValidateInterface)
		if is {
			err := validate.Validate()
			if err != nil {
				return err
			}
		} else {
			validate, is := value.Interface().(ValidateInterface)
			if is {
				err := validate.Validate()
				if err != nil {
					return err
				}
			}
		}

		orm := initIfNeeded(engine, v, true)
		isDirty, bind, err := getDirtyBind(value)
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
			insertReflectValues[t] = append(insertReflectValues[t], v)
			insertBinds[t] = append(insertBinds[t], bind)
			totalInsert[t]++
		} else {
			values := make([]interface{}, bindLength+1)
			currentID := value.Field(1).Uint()
			if orm.dBData["_delete"] == true {
				if deleteBinds[t] == nil {
					deleteBinds[t] = make(map[uint64]map[string]interface{})
				}
				deleteBinds[t][currentID] = orm.dBData
			} else {
				fields := make([]string, bindLength)
				i := 0
				for key, value := range bind {
					fields[i] = fmt.Sprintf("`%s` = ?", key)
					values[i] = value
					i++
				}
				schema := orm.tableSchema
				/* #nosec */
				sql := fmt.Sprintf("UPDATE %s SET %s WHERE `ID` = ?", schema.TableName, strings.Join(fields, ","))
				db := schema.GetMysql(engine)
				values[i] = currentID
				if lazy {
					fillLazyQuery(lazyMap, db.GetPoolCode(), sql, values)
				} else {
					_, err := db.Exec(sql, values...)
					if err != nil {
						return convertToError(err)
					}
					afterSaved, is := v.Interface().(AfterSavedInterface)
					if is {
						err := afterSaved.AfterSaved(engine)
						if err != nil {
							return err
						}
					}
				}
				old := make(map[string]interface{}, len(orm.dBData))
				for k, v := range orm.dBData {
					old[k] = v
				}
				data := injectBind(value, bind)
				localCache, hasLocalCache := schema.GetLocalCache(engine)
				redisCache, hasRedis := schema.GetRedisCacheContainer(engine)
				if hasLocalCache {
					addLocalCacheSet(localCacheSets, db.GetPoolCode(), localCache.code, schema.getCacheKey(currentID), buildLocalCacheValue(v.Interface(), schema))
					keys := getCacheQueriesKeys(schema, bind, orm.dBData, false)
					addCacheDeletes(localCacheDeletes, localCache.code, keys...)
					keys = getCacheQueriesKeys(schema, bind, old, false)
					addCacheDeletes(localCacheDeletes, localCache.code, keys...)
				}
				if hasRedis {
					addCacheDeletes(redisKeysToDelete, db.GetPoolCode(), redisCache.code, schema.getCacheKey(currentID))
					keys := getCacheQueriesKeys(schema, bind, orm.dBData, false)
					addCacheDeletes(redisKeysToDelete, redisCache.code, keys...)
					keys = getCacheQueriesKeys(schema, bind, old, false)
					addCacheDeletes(redisKeysToDelete, redisCache.code, keys...)
				}
				addDirtyQueues(dirtyQueues, bind, schema, currentID, "u")
				addToLogQueue(logQueues, schema, currentID, data)
			}
		}
	}
	for typeOf, values := range insertKeys {
		schema := getTableSchema(engine.config, typeOf)
		finalValues := make([]string, len(values))
		for key, val := range values {
			finalValues[key] = fmt.Sprintf("`%s`", val)
		}
		/* #nosec */
		sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES %s", schema.TableName, strings.Join(finalValues, ","), insertValues[typeOf])
		for i := 1; i < totalInsert[typeOf]; i++ {
			sql += "," + insertValues[typeOf]
		}
		id := uint64(0)
		db := schema.GetMysql(engine)
		if lazy {
			fillLazyQuery(lazyMap, db.GetPoolCode(), sql, insertArguments[typeOf])
		} else {
			res, err := db.Exec(sql, insertArguments[typeOf]...)
			if err != nil {
				return convertToError(err)
			}
			insertID, err := res.LastInsertId()
			if err != nil {
				return err
			}
			id = uint64(insertID)
		}
		localCache, hasLocalCache := schema.GetLocalCache(engine)
		redisCache, hasRedis := schema.GetRedisCacheContainer(engine)
		for key, value := range insertReflectValues[typeOf] {
			elem := value.Elem()
			entity := value.Interface()
			bind := insertBinds[typeOf][key]
			injectBind(elem, bind)
			elem.Field(1).SetUint(id)
			if hasLocalCache {
				if !lazy {
					addLocalCacheSet(localCacheSets, db.GetPoolCode(), localCache.code, schema.getCacheKey(id), buildLocalCacheValue(entity, schema))
				} else {
					addCacheDeletes(localCacheDeletes, db.GetPoolCode(), localCache.code, schema.getCacheKey(id))
				}
				keys := getCacheQueriesKeys(schema, bind, bind, true)
				addCacheDeletes(localCacheDeletes, localCache.code, keys...)
			}
			if hasRedis {
				addCacheDeletes(redisKeysToDelete, db.GetPoolCode(), redisCache.code, schema.getCacheKey(id))
				keys := getCacheQueriesKeys(schema, bind, bind, true)
				addCacheDeletes(redisKeysToDelete, redisCache.code, keys...)
			}
			addDirtyQueues(dirtyQueues, bind, schema, id, "i")
			addToLogQueue(logQueues, schema, id, bind)
			id++
			afterSaveInterface, is := entity.(AfterSavedInterface)
			if is {
				err := afterSaveInterface.AfterSaved(engine)
				if err != nil {
					return err
				}
			}
		}
	}
	for typeOf, deleteBinds := range deleteBinds {
		schema := getTableSchema(engine.config, typeOf)
		ids := make([]interface{}, len(deleteBinds))
		i := 0
		for id := range deleteBinds {
			ids[i] = id
			i++
		}
		/* #nosec */
		sql := fmt.Sprintf("DELETE FROM `%s` WHERE %s", schema.TableName, NewWhere("`ID` IN ?", ids))
		db := schema.GetMysql(engine)
		if lazy {
			fillLazyQuery(lazyMap, db.GetPoolCode(), sql, ids)
		} else {
			usage, err := schema.GetUsage(engine.config)
			if err != nil {
				return err
			}
			if len(usage) > 0 {
				for refT, refColumns := range usage {
					for _, refColumn := range refColumns {
						refSchema := getTableSchema(engine.config, refT)
						_, isCascade := refSchema.Tags[refColumn]["cascade"]
						if isCascade {
							subValue := reflect.New(reflect.SliceOf(reflect.PtrTo(refT)))
							subElem := subValue.Elem()
							sub := subValue.Interface()
							pager := &Pager{CurrentPage: 1, PageSize: 1000}
							where := NewWhere(fmt.Sprintf("`%s` IN ?", refColumn), ids)
							max := 10
							for {
								err := engine.Search(where, pager, sub)
								if err != nil {
									return err
								}
								total := subElem.Len()
								if total == 0 {
									break
								}
								toDeleteAll := make([]reflect.Value, total)
								for i := 0; i < total; i++ {
									toDeleteValue := subElem.Index(i)
									toDeleteValue.Elem().Field(0).Addr().Interface().(*ORM).MarkToDelete()
									toDeleteAll[i] = toDeleteValue
								}
								err = flush(engine, lazy, toDeleteAll...)
								if err != nil {
									return err
								}
								max--
								if max == 0 {
									return fmt.Errorf("cascade limit exceeded")
								}
							}
						}
					}
				}
			}
			_, err = db.Exec(sql, ids...)
			if err != nil {
				return convertToError(err)
			}
		}

		localCache, hasLocalCache := schema.GetLocalCache(engine)
		redisCache, hasRedis := schema.GetRedisCacheContainer(engine)
		if hasLocalCache {
			for id, bind := range deleteBinds {
				addLocalCacheSet(localCacheSets, db.GetPoolCode(), localCache.code, schema.getCacheKey(id), "nil")
				keys := getCacheQueriesKeys(schema, bind, bind, true)
				addCacheDeletes(localCacheDeletes, localCache.code, keys...)
			}
		}
		if hasRedis {
			for id, bind := range deleteBinds {
				addCacheDeletes(redisKeysToDelete, db.GetPoolCode(), redisCache.code, schema.getCacheKey(id))
				keys := getCacheQueriesKeys(schema, bind, bind, true)
				addCacheDeletes(redisKeysToDelete, redisCache.code, keys...)
			}
		}
		for id, bind := range deleteBinds {
			addDirtyQueues(dirtyQueues, bind, schema, id, "d")
			addToLogQueue(logQueues, schema, id, nil)
		}
	}
	for _, values := range localCacheSets {
		for cacheCode, keys := range values {
			cache, _ := engine.GetLocalCache(cacheCode)
			cache.MSet(keys...)
		}
	}
	for cacheCode, allKeys := range localCacheDeletes {
		cache, _ := engine.GetLocalCache(cacheCode)
		keys := make([]string, len(allKeys))
		i := 0
		for key := range allKeys {
			keys[i] = key
			i++
		}
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
	}
	for cacheCode, allKeys := range redisKeysToDelete {
		cache, _ := engine.GetRedis(cacheCode)
		keys := make([]string, len(allKeys))
		i := 0
		for key := range allKeys {
			keys[i] = key
			i++
		}
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
	}
	if len(lazyMap) > 0 {
		v, err := serializeForLazyQueue(lazyMap)
		if err != nil {
			return err
		}
		code := "default"
		redis, _ := engine.getRedisForQueue(code)
		_, err = redis.LPush("lazy_queue", v)
		if err != nil {
			return err
		}
	}

	for k, v := range dirtyQueues {
		if engine.config.dirtyQueues == nil {
			return fmt.Errorf("unregistered lazy queue %s", k)
		}
		queue, has := engine.config.dirtyQueues[k]
		if !has {
			return fmt.Errorf("unregistered lazy queue %s", k)
		}
		err := queue.Send(engine, k, v)
		if err != nil {
			return err
		}
	}
	for k, v := range logQueues {
		if engine.config.logQueues == nil {
			return fmt.Errorf("unregistered log queue %s", k)
		}
		queue, has := engine.config.logQueues[k]
		if !has {
			return fmt.Errorf("unregistered log queue %s", k)
		}
		err := queue.Send(engine, v)
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

func injectBind(value reflect.Value, bind map[string]interface{}) map[string]interface{} {
	field := value.Field(0)
	oldFields := field.Interface().(ORM)
	for key, value := range bind {
		oldFields.dBData[key] = value
	}
	field.Set(reflect.ValueOf(oldFields))
	return oldFields.dBData
}

func createBind(id uint64, tableSchema *TableSchema, t reflect.Type, value reflect.Value,
	oldData map[string]interface{}, prefix string) (bind map[string]interface{}, err error) {
	bind = make(map[string]interface{})
	var hasOld = len(oldData) > 0
	for i := 0; i < t.NumField(); i++ {
		fieldType := t.Field(i)
		name := prefix + fieldType.Name
		if prefix == "" && i <= 1 {
			continue
		}
		old := oldData[name]
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
			if attributes["year"] == "true" {
				valString = fmt.Sprintf("%04d", val)
				if hasOld && (old == valString || (valString == "0000" && (old == nil || old == ""))) {
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
				bind[name] = valueAsString
			}
		case "bool":
			if name == "FakeDelete" {
				value := "0"
				if field.Bool() {
					if id == 0 {
						return nil, fmt.Errorf("fake delete not allowed for new row")
					}
					value = strconv.FormatUint(id, 10)
				}
				if hasOld && old == value {
					continue
				}
				bind[name] = value
				continue
			}
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
		case "*orm.CachedQuery":
			continue
		case "time.Time":
			value := field.Interface().(time.Time)
			layout := "2006-01-02"
			var valueAsString string
			if tableSchema.Tags[name]["time"] == "true" {
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
			if hasOld && old == valueAsString {
				continue
			}
			bind[name] = valueAsString
			continue
		case "*time.Time":
			value := field.Interface().(*time.Time)
			layout := "2006-01-02"
			var valueAsString string
			if tableSchema.Tags[name]["time"] == "true" {
				if value != nil {
					layout += " 15:04:05"
				}
			}
			if value != nil {
				valueAsString = value.Format(layout)
			}
			if hasOld && (old == valueAsString || (valueAsString == "" && (old == nil || old == ""))) {
				continue
			}
			if valueAsString == "" {
				bind[name] = nil
			} else {
				bind[name] = valueAsString
			}
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
			k := field.Kind().String()
			if k == "struct" {
				subBind, err := createBind(0, tableSchema, field.Type(), reflect.ValueOf(field.Interface()), oldData, fieldType.Name)
				if err != nil {
					return nil, err
				}
				for key, value := range subBind {
					bind[key] = value
				}
				continue
			} else if k == "ptr" {
				valueAsString := "0"
				if !field.IsNil() {
					valueAsString = strconv.FormatUint(field.Elem().Field(1).Uint(), 10)
				}
				if hasOld && (old == valueAsString || (old == nil && valueAsString == "0")) {
					continue
				}
				if valueAsString == "0" {
					bind[name] = nil
				} else {
					bind[name] = valueAsString
				}
				continue
			}
			return nil, fmt.Errorf("unsupported field type: %s", field.Type().String())
		}
	}
	return
}

func getCacheQueriesKeys(schema *TableSchema, bind map[string]interface{}, data map[string]interface{}, addedDeleted bool) (keys []string) {
	keys = make([]string, 0)

	for indexName, definition := range schema.cachedIndexesAll {
		if !addedDeleted && schema.hasFakeDelete {
			_, addedDeleted = bind["FakeDelete"]
		}
		if addedDeleted && len(definition.Fields) == 0 {
			keys = append(keys, schema.getCacheKeySearch(indexName))
		}
		for _, trackedField := range definition.Fields {
			_, has := bind[trackedField]
			if has {
				attributes := make([]interface{}, 0)
				for _, trackedFieldSub := range definition.Fields {
					val := data[trackedFieldSub]
					if !schema.hasFakeDelete || trackedFieldSub != "FakeDelete" {
						attributes = append(attributes, val)
					}
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

func addCacheDeletes(cacheDeletes map[string]map[string]bool, cacheCode string, keys ...string) {
	if len(keys) == 0 {
		return
	}
	if cacheDeletes[cacheCode] == nil {
		cacheDeletes[cacheCode] = make(map[string]bool)
	}
	for _, key := range keys {
		cacheDeletes[cacheCode][key] = true
	}
}

func addDirtyQueues(keys map[string][]*DirtyQueueValue, bind map[string]interface{}, schema *TableSchema, id uint64, action string) {
	results := make(map[string]*DirtyQueueValue)
	key := &DirtyQueueValue{EntityName: schema.t.String(), ID: id, Added: action == "i", Updated: action == "u", Deleted: action == "d"}
	for column, tags := range schema.Tags {
		queues, has := tags["dirty"]
		if !has {
			continue
		}
		isDirty := column == "ORM"
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

func addToLogQueue(keys map[string][]*LogQueueValue, tableSchema *TableSchema, id uint64, data map[string]interface{}) {
	if !tableSchema.hasLog {
		return
	}
	key := tableSchema.logPoolName
	if keys[key] == nil {
		keys[key] = make([]*LogQueueValue, 0)
	}
	val := &LogQueueValue{TableName: tableSchema.logTableName, ID: id,
		PoolName: tableSchema.logPoolName, Data: data, Updated: time.Now()}
	keys[key] = append(keys[key], val)
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

func convertToError(err error) error {
	sqlErr, yes := err.(*mysql.MySQLError)
	if yes {
		if sqlErr.Number == 1062 {
			var abortLabelReg, _ = regexp.Compile(` for key '(.*?)'`)
			labels := abortLabelReg.FindStringSubmatch(sqlErr.Message)
			if len(labels) > 0 {
				return &DuplicatedKeyError{Message: sqlErr.Message, Index: labels[1]}
			}
		} else if sqlErr.Number == 1451 || sqlErr.Number == 1452 {
			var abortLabelReg, _ = regexp.Compile(" CONSTRAINT `(.*?)`")
			labels := abortLabelReg.FindStringSubmatch(sqlErr.Message)
			if len(labels) > 0 {
				return &ForeignKeyError{Message: sqlErr.Message, Constraint: labels[1]}
			}
		}
	}
	return err
}
