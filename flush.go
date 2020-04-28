package orm

import (
	"encoding/json"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/juju/errors"

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

func flush(engine *Engine, lazy bool, transaction bool, entities ...reflect.Value) error {
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

	var referencesToFlash map[reflect.Value]reflect.Value

	for _, v := range entities {
		if !v.IsValid() {
			return fmt.Errorf("unregistered struct '%s'. run engine.RegisterEntity(entity) before entity.Flush()", v.Kind().String())
		}
		entity, ok := v.Interface().(Entity)
		if !ok {
			return fmt.Errorf("invalid entity '%s'", v.Type().String())
		}
		schema := entity.getORM().tableSchema
		for _, refName := range schema.refOne {
			refValue := entity.getORM().attributes.elem.FieldByName(refName)
			ref := refValue.Interface().(Entity)
			if !refValue.IsNil() && refValue.Elem().Field(1).Uint() == 0 {
				if referencesToFlash == nil {
					referencesToFlash = make(map[reflect.Value]reflect.Value)
				}
				referencesToFlash[ref.getORM().attributes.value] = ref.getORM().attributes.value
			}
		}
		if referencesToFlash != nil {
			continue
		}

		orm := entity.getORM()
		dbData := orm.dBData
		value := reflect.Indirect(v)
		isDirty, bind, err := getDirtyBind(v.Interface().(Entity))
		if err != nil {
			return err
		}
		if !isDirty {
			continue
		}
		bindLength := len(bind)

		t := value.Type()
		currentID := value.Field(1).Uint()
		if orm.attributes.delete {
			if deleteBinds[t] == nil {
				deleteBinds[t] = make(map[uint64]map[string]interface{})
			}
			deleteBinds[t][currentID] = dbData
		} else if len(dbData) == 0 {
			if currentID > 0 {
				return fmt.Errorf("unloaded entity %s with ID %d", value.Type().String(), currentID)
			}
			onUpdate := entity.getORM().attributes.onDuplicateKeyUpdate
			if onUpdate != nil {
				values := make([]string, bindLength)
				columns := make([]string, bindLength)
				bindRow := make([]interface{}, bindLength)
				i := 0
				for key, val := range bind {
					columns[i] = fmt.Sprintf("`%s`", key)
					values[i] = "?"
					bindRow[i] = val
					i++
				}
				/* #nosec */
				sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES (%s)", schema.tableName, strings.Join(columns, ","), strings.Join(values, ","))
				sql += " ON DUPLICATE KEY UPDATE "
				subSQL := onUpdate.String()
				if subSQL == "" {
					subSQL = "`Id` = `Id`"
				}
				sql += subSQL
				bindRow = append(bindRow, onUpdate.GetParameters()...)
				db := schema.GetMysql(engine)
				if lazy {
					fillLazyQuery(lazyMap, db.GetPoolCode(), sql, bindRow)
				} else {
					result, err := db.Exec(sql, bindRow...)
					if err != nil {
						return convertToError(err)
					}
					affected, err := result.RowsAffected()
					if err != nil {
						return err
					}
					var lastID int64
					if affected > 0 {
						lastID, err = result.LastInsertId()
						if err != nil {
							return err
						}
					}
					if affected > 0 {
						injectBind(entity.getORM().attributes.value, bind)
						entity.getORM().attributes.elem.Field(1).SetUint(uint64(lastID))
						updateCacheForInserted(entity, lazy, uint64(lastID), bind, localCacheSets, localCacheDeletes, redisKeysToDelete, dirtyQueues, logQueues)
						if affected == 2 {
							_, err = loadByID(engine, uint64(lastID), entity, false)
							if err != nil {
								return err
							}
						}
					} else {
						valid := false
						for _, index := range schema.uniqueIndices {
							allNotNil := true
							fields := make([]string, 0)
							binds := make([]interface{}, 0)
							for _, column := range index {
								if bind[column] == nil {
									allNotNil = false
									break
								}
								fields = append(fields, fmt.Sprintf("`%s` = ?", column))
								binds = append(binds, bind[column])
							}
							if allNotNil {
								findWhere := NewWhere(strings.Join(fields, " AND "), binds)
								has, err := engine.SearchOne(findWhere, entity)
								if err != nil {
									return err
								}
								if !has {
									return fmt.Errorf("missing unique index to find updated row")
								}
								valid = true
								break
							}
						}
						if !valid {
							return fmt.Errorf("missing unique index to find updated row")
						}
					}
				}
				continue
			}

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
			if !engine.Loaded(entity) {
				return fmt.Errorf("entity is not loaded and can't be updated: %v [%d]", entity.getORM().attributes.elem.Type().String(), currentID)
			}
			fields := make([]string, bindLength)
			i := 0
			for key, value := range bind {
				fields[i] = fmt.Sprintf("`%s` = ?", key)
				values[i] = value
				i++
			}
			/* #nosec */
			sql := fmt.Sprintf("UPDATE %s SET %s WHERE `ID` = ?", schema.GetTableName(), strings.Join(fields, ","))
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
			old := make(map[string]interface{}, len(dbData))
			for k, v := range dbData {
				old[k] = v
			}
			data := injectBind(v, bind)
			localCache, hasLocalCache := schema.GetLocalCache(engine)
			redisCache, hasRedis := schema.GetRedisCache(engine)
			if hasLocalCache {
				addLocalCacheSet(localCacheSets, db.GetPoolCode(), localCache.code, schema.getCacheKey(currentID), buildLocalCacheValue(entity))
				keys := getCacheQueriesKeys(schema, bind, dbData, false)
				addCacheDeletes(localCacheDeletes, localCache.code, keys...)
				keys = getCacheQueriesKeys(schema, bind, old, false)
				addCacheDeletes(localCacheDeletes, localCache.code, keys...)
			}
			if hasRedis {
				addCacheDeletes(redisKeysToDelete, redisCache.code, schema.getCacheKey(currentID))
				keys := getCacheQueriesKeys(schema, bind, dbData, false)
				addCacheDeletes(redisKeysToDelete, redisCache.code, keys...)
				keys = getCacheQueriesKeys(schema, bind, old, false)
				addCacheDeletes(redisKeysToDelete, redisCache.code, keys...)
			}
			addDirtyQueues(dirtyQueues, bind, schema, currentID, "u")
			addToLogQueue(logQueues, schema, currentID, data)
		}
	}

	if referencesToFlash != nil {
		if lazy {
			return fmt.Errorf("lazy flush not supported for unsaved regerences")
		}
		toFlush := make([]reflect.Value, len(referencesToFlash))
		i := 0
		for _, v := range referencesToFlash {
			toFlush[i] = v
			i++
		}
		err := flush(engine, false, transaction, toFlush...)
		if err != nil {
			return err
		}
		rest := make([]reflect.Value, 0)
		for _, v := range entities {
			_, has := referencesToFlash[v]
			if !has {
				rest = append(rest, v)
			}
		}
		err = flush(engine, transaction, false, rest...)
		if err != nil {
			return err
		}
		return nil
	}
	for typeOf, values := range insertKeys {
		schema := getTableSchema(engine.registry, typeOf)
		finalValues := make([]string, len(values))
		for key, val := range values {
			finalValues[key] = fmt.Sprintf("`%s`", val)
		}
		/* #nosec */
		sql := fmt.Sprintf("INSERT INTO %s(%s) VALUES %s", schema.tableName, strings.Join(finalValues, ","), insertValues[typeOf])
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
		for key, value := range insertReflectValues[typeOf] {
			elem := value.Elem()
			entity := value.Interface().(Entity)
			bind := insertBinds[typeOf][key]
			injectBind(value, bind)
			elem.Field(1).SetUint(id)
			updateCacheForInserted(entity, lazy, id, bind, localCacheSets, localCacheDeletes, redisKeysToDelete, dirtyQueues, logQueues)
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
		schema := getTableSchema(engine.registry, typeOf)
		ids := make([]interface{}, len(deleteBinds))
		i := 0
		for id := range deleteBinds {
			ids[i] = id
			i++
		}
		/* #nosec */
		sql := fmt.Sprintf("DELETE FROM `%s` WHERE %s", schema.tableName, NewWhere("`ID` IN ?", ids))
		db := schema.GetMysql(engine)
		if lazy {
			fillLazyQuery(lazyMap, db.GetPoolCode(), sql, ids)
		} else {
			usage, err := schema.GetUsage(engine.registry)
			if err != nil {
				return err
			}
			if len(usage) > 0 {
				for refT, refColumns := range usage {
					for _, refColumn := range refColumns {
						refSchema := getTableSchema(engine.registry, refT)
						_, isCascade := refSchema.tags[refColumn]["cascade"]
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
									engine.MarkToDelete(toDeleteValue.Interface().(Entity))
									toDeleteAll[i] = toDeleteValue
								}
								err = flush(engine, transaction, lazy, toDeleteAll...)
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
		redisCache, hasRedis := schema.GetRedisCache(engine)
		if hasLocalCache {
			for id, bind := range deleteBinds {
				addLocalCacheSet(localCacheSets, db.GetPoolCode(), localCache.code, schema.getCacheKey(id), "nil")
				keys := getCacheQueriesKeys(schema, bind, bind, true)
				addCacheDeletes(localCacheDeletes, localCache.code, keys...)
			}
		}
		if hasRedis {
			for id, bind := range deleteBinds {
				addCacheDeletes(redisKeysToDelete, redisCache.code, schema.getCacheKey(id))
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
			cache := engine.GetLocalCache(cacheCode)
			if !transaction {
				cache.MSet(keys...)
			} else {
				if engine.afterCommitLocalCacheSets == nil {
					engine.afterCommitLocalCacheSets = make(map[string][]interface{})
				}
				engine.afterCommitLocalCacheSets[cacheCode] = append(engine.afterCommitLocalCacheSets[cacheCode], keys...)
			}
		}
	}
	for cacheCode, allKeys := range localCacheDeletes {
		cache := engine.GetLocalCache(cacheCode)
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
		cache := engine.GetRedis(cacheCode)
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
			if !transaction {
				err := cache.Del(keys...)
				if err != nil {
					return err
				}
			} else {
				if engine.afterCommitRedisCacheDeletes == nil {
					engine.afterCommitRedisCacheDeletes = make(map[string][]string)
				}
				engine.afterCommitRedisCacheDeletes[cacheCode] = append(engine.afterCommitRedisCacheDeletes[cacheCode], keys...)
			}
		}
	}
	if len(lazyMap) > 0 {
		v, err := serializeForLazyQueue(lazyMap)
		if err != nil {
			return err
		}
		code := "default"
		if engine.registry.lazyQueues == nil {
			return fmt.Errorf("unregistered lazy queue")
		}
		queue, has := engine.registry.lazyQueues[code]
		if !has {
			return fmt.Errorf("unregistered log queu")
		}
		err = queue.Send(engine, lazyQueueName, []string{v})
		if err != nil {
			return err
		}
	}

	for k, v := range dirtyQueues {
		if engine.registry.dirtyQueues == nil {
			return fmt.Errorf("unregistered lazy queue %s", k)
		}
		queue, has := engine.registry.dirtyQueues[k]
		if !has {
			return fmt.Errorf("unregistered lazy queue %s", k)
		}
		err := queue.Send(engine, k, v)
		if err != nil {
			return err
		}
	}
	for k, v := range logQueues {
		if engine.registry.logQueues == nil {
			return fmt.Errorf("unregistered log queue %s", k)
		}
		queue, has := engine.registry.logQueues[k]
		if !has {
			return fmt.Errorf("unregistered log queue %s", k)
		}

		members := make([]string, len(v))
		for i, val := range v {
			val.Meta = engine.logMetaData
			asJSON, err := json.Marshal(val)
			if err != nil {
				return errors.Trace(err)
			}
			members[i] = string(asJSON)
		}
		err := queue.Send(engine, logQueueName, members)
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
	orm := value.Interface().(Entity).getORM()
	for key, value := range bind {
		orm.dBData[key] = value
	}
	orm.attributes.loaded = true
	return orm.dBData
}

func createBind(id uint64, tableSchema *tableSchema, t reflect.Type, value reflect.Value,
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
		attributes := tableSchema.tags[name]
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
			fieldAttributes := tableSchema.tags[name]
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
			if tableSchema.tags[name]["time"] == "true" {
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
			if tableSchema.tags[name]["time"] == "true" {
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

func getCacheQueriesKeys(schema *tableSchema, bind map[string]interface{}, data map[string]interface{}, addedDeleted bool) (keys []string) {
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

func addDirtyQueues(keys map[string][]*DirtyQueueValue, bind map[string]interface{}, schema *tableSchema, id uint64, action string) {
	results := make(map[string]*DirtyQueueValue)
	key := &DirtyQueueValue{EntityName: schema.t.String(), ID: id, Added: action == "i", Updated: action == "u", Deleted: action == "d"}
	for column, tags := range schema.tags {
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

func addToLogQueue(keys map[string][]*LogQueueValue, tableSchema *tableSchema, id uint64, data map[string]interface{}) {
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

func updateCacheForInserted(entity Entity, lazy bool, id uint64,
	bind map[string]interface{}, localCacheSets map[string]map[string][]interface{}, localCacheDeletes map[string]map[string]bool,
	redisKeysToDelete map[string]map[string]bool, dirtyQueues map[string][]*DirtyQueueValue,
	logQueues map[string][]*LogQueueValue) {
	schema := entity.getORM().tableSchema
	engine := entity.getORM().engine
	localCache, hasLocalCache := schema.GetLocalCache(engine)
	redisCache, hasRedis := schema.GetRedisCache(engine)
	if hasLocalCache {
		if !lazy {
			addLocalCacheSet(localCacheSets, schema.GetMysql(engine).GetPoolCode(), localCache.code, schema.getCacheKey(id), buildLocalCacheValue(entity))
		} else {
			addCacheDeletes(localCacheDeletes, localCache.code, schema.getCacheKey(id))
		}
		keys := getCacheQueriesKeys(schema, bind, bind, true)
		addCacheDeletes(localCacheDeletes, localCache.code, keys...)
	}
	if hasRedis {
		addCacheDeletes(redisKeysToDelete, redisCache.code, schema.getCacheKey(id))
		keys := getCacheQueriesKeys(schema, bind, bind, true)
		addCacheDeletes(redisKeysToDelete, redisCache.code, keys...)
	}
	addDirtyQueues(dirtyQueues, bind, schema, id, "i")
	addToLogQueue(logQueues, schema, id, bind)
}
