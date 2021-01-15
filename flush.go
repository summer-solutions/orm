package orm

import (
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"

	"github.com/juju/errors"

	jsoniter "github.com/json-iterator/go"

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

type dataLoaderSets map[*tableSchema]map[uint64][]string

func flush(engine *Engine, lazy bool, transaction bool, smart bool, entities ...Entity) {
	insertKeys := make(map[reflect.Type][]string)
	insertValues := make(map[reflect.Type]string)
	insertArguments := make(map[reflect.Type][]interface{})
	insertBinds := make(map[reflect.Type][]map[string]interface{})
	insertReflectValues := make(map[reflect.Type][]Entity)
	deleteBinds := make(map[reflect.Type]map[uint64]map[string]interface{})
	totalInsert := make(map[reflect.Type]int)
	localCacheSets := make(map[string]map[string][]interface{})
	dataLoaderSets := make(map[*tableSchema]map[uint64][]string)
	localCacheDeletes := make(map[string]map[string]bool)
	redisKeysToDelete := make(map[string]map[string]bool)
	dirtyChannels := make(map[string][]EventAsMap)
	logQueues := make([]*LogQueueValue, 0)
	lazyMap := make(map[string]interface{})
	isInTransaction := transaction

	var referencesToFlash map[Entity]Entity

	for _, entity := range entities {
		schema := entity.getORM().tableSchema
		if !isInTransaction && schema.GetMysql(engine).inTransaction {
			isInTransaction = true
		}
		for _, refName := range schema.refOne {
			refValue := entity.getORM().attributes.elem.FieldByName(refName)
			if refValue.IsValid() && !refValue.IsNil() {
				refEntity := refValue.Interface().(Entity)
				initIfNeeded(engine, refEntity)
				if refEntity.GetID() == 0 {
					if referencesToFlash == nil {
						referencesToFlash = make(map[Entity]Entity)
					}
					referencesToFlash[refEntity] = refEntity
				}
			}
		}
		for _, refName := range schema.refMany {
			refValue := entity.getORM().attributes.elem.FieldByName(refName)
			if refValue.IsValid() && !refValue.IsNil() {
				length := refValue.Len()
				for i := 0; i < length; i++ {
					refEntity := refValue.Index(i).Interface().(Entity)
					initIfNeeded(engine, refEntity)
					if refEntity.GetID() == 0 {
						if referencesToFlash == nil {
							referencesToFlash = make(map[Entity]Entity)
						}
						referencesToFlash[refEntity] = refEntity
					}
				}
			}
		}
		if referencesToFlash != nil {
			continue
		}

		orm := entity.getORM()
		dbData := orm.dBData
		isDirty, bind := getDirtyBind(entity)
		if !isDirty {
			continue
		}
		bindLength := len(bind)

		t := orm.tableSchema.t
		currentID := entity.GetID()
		if orm.attributes.delete {
			if deleteBinds[t] == nil {
				deleteBinds[t] = make(map[uint64]map[string]interface{})
			}
			deleteBinds[t][currentID] = dbData
		} else if len(dbData) == 0 {
			onUpdate := entity.getORM().attributes.onDuplicateKeyUpdate
			if onUpdate != nil {
				if lazy {
					panic(errors.NotSupportedf("lazy flush on duplicate key"))
				}
				if currentID > 0 {
					bind["ID"] = currentID
					bindLength++
				}
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
				result := db.Exec(sql, bindRow...)
				affected := result.RowsAffected()
				if affected > 0 && currentID == 0 {
					lastID := result.LastInsertId()
					injectBind(entity, bind)
					entity.getORM().attributes.idElem.SetUint(lastID)
					if affected == 1 {
						logQueues = updateCacheForInserted(entity, lazy, lastID, bind, localCacheSets,
							localCacheDeletes, redisKeysToDelete, dirtyChannels, logQueues, dataLoaderSets)
					} else {
						_ = loadByID(engine, lastID, entity, false)
						logQueues = updateCacheAfterUpdate(dbData, engine, entity, bind, schema, localCacheSets, localCacheDeletes, db, lastID,
							redisKeysToDelete, dirtyChannels, logQueues, dataLoaderSets)
					}
				} else if currentID > 0 {
					_ = loadByID(engine, currentID, entity, false)
					logQueues = updateCacheForInserted(entity, lazy, currentID, bind, localCacheSets,
						localCacheDeletes, redisKeysToDelete, dirtyChannels, logQueues, dataLoaderSets)
				} else {
				OUTER:
					for _, index := range schema.uniqueIndices {
						fields := make([]string, 0)
						binds := make([]interface{}, 0)
						for _, column := range index {
							if bind[column] == nil {
								continue OUTER
							}
							fields = append(fields, fmt.Sprintf("`%s` = ?", column))
							binds = append(binds, bind[column])
						}
						findWhere := NewWhere(strings.Join(fields, " AND "), binds)
						engine.SearchOne(findWhere, entity)
						break
					}
				}
				continue
			}
			if currentID > 0 {
				bind["ID"] = currentID
				bindLength++
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
				insertReflectValues[t] = make([]Entity, 0)
				insertBinds[t] = make([]map[string]interface{}, 0)
				insertValues[t] = fmt.Sprintf("(%s)", strings.Join(valuesKeys, ","))
			}
			insertArguments[t] = append(insertArguments[t], values...)
			insertReflectValues[t] = append(insertReflectValues[t], entity)
			insertBinds[t] = append(insertBinds[t], bind)
			totalInsert[t]++
		} else {
			values := make([]interface{}, bindLength+1)
			if !engine.Loaded(entity) {
				panic(errors.Errorf("entity is not loaded and can't be updated: %v [%d]", entity.getORM().attributes.elem.Type().String(), currentID))
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
				smartUpdate := false
				if smart && !db.inTransaction && schema.hasLocalCache && !schema.hasRedisCache {
					keys := getCacheQueriesKeys(schema, bind, dbData, false)
					smartUpdate = len(keys) == 0
				}
				if smartUpdate {
					fillLazyQuery(lazyMap, db.GetPoolCode(), sql, values)
				} else {
					_ = db.Exec(sql, values...)
				}
			}
			logQueues = updateCacheAfterUpdate(dbData, engine, entity, bind, schema, localCacheSets, localCacheDeletes, db, currentID,
				redisKeysToDelete, dirtyChannels, logQueues, dataLoaderSets)
		}
	}

	if referencesToFlash != nil {
		if lazy {
			panic(errors.NotSupportedf("lazy flush for unsaved references"))
		}
		toFlush := make([]Entity, len(referencesToFlash))
		i := 0
		for _, v := range referencesToFlash {
			toFlush[i] = v
			i++
		}
		flush(engine, false, transaction, false, toFlush...)
		rest := make([]Entity, 0)
		for _, v := range entities {
			_, has := referencesToFlash[v]
			if !has {
				rest = append(rest, v)
			}
		}
		flush(engine, false, transaction, false, rest...)
		return
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
			res := db.Exec(sql, insertArguments[typeOf]...)
			id = res.LastInsertId()
		}
		for key, entity := range insertReflectValues[typeOf] {
			bind := insertBinds[typeOf][key]
			injectBind(entity, bind)
			insertedID := entity.GetID()
			if insertedID == 0 {
				entity.getORM().attributes.idElem.SetUint(id)
				insertedID = id
				id = id + db.autoincrement
			}
			logQueues = updateCacheForInserted(entity, lazy, insertedID, bind, localCacheSets, localCacheDeletes,
				redisKeysToDelete, dirtyChannels, logQueues, dataLoaderSets)
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
			usage := schema.GetUsage(engine.registry)
			if len(usage) > 0 {
				for refT, refColumns := range usage {
					for _, refColumn := range refColumns {
						refSchema := getTableSchema(engine.registry, refT)
						_, isCascade := refSchema.tags[refColumn]["cascade"]
						if isCascade {
							subValue := reflect.New(reflect.SliceOf(reflect.PtrTo(refT)))
							subElem := subValue.Elem()
							sub := subValue.Interface()
							pager := NewPager(1, 1000)
							where := NewWhere(fmt.Sprintf("`%s` IN ?", refColumn), ids)
							for {
								engine.Search(where, pager, sub)
								total := subElem.Len()
								if total == 0 {
									break
								}
								toDeleteAll := make([]Entity, total)
								for i := 0; i < total; i++ {
									toDeleteValue := subElem.Index(i).Interface().(Entity)
									engine.MarkToDelete(toDeleteValue)
									toDeleteAll[i] = toDeleteValue
								}
								flush(engine, transaction, lazy, false, toDeleteAll...)
							}
						}
					}
				}
			}
			_ = db.Exec(sql, ids...)
		}

		localCache, hasLocalCache := schema.GetLocalCache(engine)
		redisCache, hasRedis := schema.GetRedisCache(engine)
		if !hasLocalCache && engine.hasRequestCache {
			hasLocalCache = true
			localCache = engine.GetLocalCache(requestCacheKey)
		}
		if hasLocalCache {
			for id, bind := range deleteBinds {
				addLocalCacheSet(localCacheSets, db.GetPoolCode(), localCache.code, schema.getCacheKey(id), "nil")
				keys := getCacheQueriesKeys(schema, bind, bind, true)
				addCacheDeletes(localCacheDeletes, localCache.code, keys...)
			}
		} else if engine.dataLoader != nil {
			for id := range deleteBinds {
				addToDataLoader(dataLoaderSets, schema, id, nil)
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
			addDirtyQueues(dirtyChannels, bind, schema, id, "d")
			logQueues = addToLogQueue(logQueues, schema, id, bind, nil, nil)
		}
	}
	for _, values := range localCacheSets {
		for cacheCode, keys := range values {
			cache := engine.GetLocalCache(cacheCode)
			if !isInTransaction {
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
			engine.GetLocalCache(cacheCode).Remove(keys...)
		}
	}
	for cacheCode, allKeys := range redisKeysToDelete {
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
			cache := engine.GetRedis(cacheCode)
			if !isInTransaction {
				cache.Del(keys...)
			} else {
				if engine.afterCommitRedisCacheDeletes == nil {
					engine.afterCommitRedisCacheDeletes = make(map[string][]string)
				}
				engine.afterCommitRedisCacheDeletes[cacheCode] = append(engine.afterCommitRedisCacheDeletes[cacheCode], keys...)
			}
		}
	}
	for schema, rows := range dataLoaderSets {
		if !isInTransaction {
			for id, value := range rows {
				engine.dataLoader.Prime(schema, id, value)
			}
		} else {
			if engine.afterCommitDataLoaderSets == nil {
				engine.afterCommitDataLoaderSets = make(map[*tableSchema]map[uint64][]string)
			}
			if engine.afterCommitDataLoaderSets[schema] == nil {
				engine.afterCommitDataLoaderSets[schema] = make(map[uint64][]string)
			}
			for id, value := range rows {
				engine.afterCommitDataLoaderSets[schema][id] = value
			}
		}
	}
	if len(lazyMap) > 0 {
		engine.GetEventBroker().Publish(lazyChannelName, lazyMap)
	}
	if !isInTransaction {
		addElementsToDirtyQueues(engine, dirtyChannels)
		addElementsToLogQueues(engine, logQueues)
	} else {
		if engine.afterCommitDirtyQueues == nil {
			engine.afterCommitDirtyQueues = make(map[string][]EventAsMap)
		}
		for key, values := range dirtyChannels {
			if engine.afterCommitDirtyQueues[key] == nil {
				engine.afterCommitDirtyQueues[key] = make([]EventAsMap, 0)
			}
			engine.afterCommitDirtyQueues[key] = append(engine.afterCommitDirtyQueues[key], values...)
		}
		if engine.afterCommitLogQueues == nil {
			engine.afterCommitLogQueues = make([]*LogQueueValue, 0)
		}
		engine.afterCommitLogQueues = append(engine.afterCommitLogQueues, logQueues...)
	}
}

func updateCacheAfterUpdate(dbData map[string]interface{}, engine *Engine, entity Entity, bind map[string]interface{},
	schema *tableSchema, localCacheSets map[string]map[string][]interface{}, localCacheDeletes map[string]map[string]bool,
	db *DB, currentID uint64, redisKeysToDelete map[string]map[string]bool,
	dirtyChannels map[string][]EventAsMap, logQueues []*LogQueueValue, dataLoaderSets dataLoaderSets) []*LogQueueValue {
	old := make(map[string]interface{}, len(dbData))
	for k, v := range dbData {
		old[k] = v
	}
	injectBind(entity, bind)
	localCache, hasLocalCache := schema.GetLocalCache(engine)
	if !hasLocalCache && engine.hasRequestCache {
		hasLocalCache = true
		localCache = engine.GetLocalCache(requestCacheKey)
	}
	redisCache, hasRedis := schema.GetRedisCache(engine)
	if hasLocalCache {
		addLocalCacheSet(localCacheSets, db.GetPoolCode(), localCache.code, schema.getCacheKey(currentID), buildLocalCacheValue(entity))
		keys := getCacheQueriesKeys(schema, bind, dbData, false)
		addCacheDeletes(localCacheDeletes, localCache.code, keys...)
		keys = getCacheQueriesKeys(schema, bind, old, false)
		addCacheDeletes(localCacheDeletes, localCache.code, keys...)
	} else if engine.dataLoader != nil {
		addToDataLoader(dataLoaderSets, schema, currentID, buildLocalCacheValue(entity))
	}
	if hasRedis {
		addCacheDeletes(redisKeysToDelete, redisCache.code, schema.getCacheKey(currentID))
		keys := getCacheQueriesKeys(schema, bind, dbData, false)
		addCacheDeletes(redisKeysToDelete, redisCache.code, keys...)
		keys = getCacheQueriesKeys(schema, bind, old, false)
		addCacheDeletes(redisKeysToDelete, redisCache.code, keys...)
	}
	addDirtyQueues(dirtyChannels, bind, schema, currentID, "u")
	return addToLogQueue(logQueues, schema, currentID, old, bind, entity.getORM().attributes.logMeta)
}

func injectBind(entity Entity, bind map[string]interface{}) map[string]interface{} {
	orm := entity.getORM()
	for key, value := range bind {
		orm.dBData[key] = value
	}
	orm.attributes.loaded = true
	return orm.dBData
}

func createBind(id uint64, tableSchema *tableSchema, t reflect.Type, value reflect.Value,
	oldData map[string]interface{}, prefix string) (bind map[string]interface{}) {
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
		fieldTypeString := field.Type().String()
		required, hasRequired := attributes["required"]
		isRequired := hasRequired && required == "true"
		switch fieldTypeString {
		case "uint", "uint8", "uint16", "uint32", "uint64":
			val := field.Uint()
			valString := strconv.FormatUint(val, 10)
			if attributes["year"] == "true" {
				valString = fmt.Sprintf("%04d", val)
				if hasOld && old == valString {
					continue
				}
				bind[name] = valString
				continue
			}
			if hasOld && old == valString {
				continue
			}
			bind[name] = valString
		case "*uint", "*uint8", "*uint16", "*uint32", "*uint64":
			if attributes["year"] == "true" {
				isNil := field.IsZero()
				if isNil {
					if hasOld && (old == "nil" || old == nil) {
						continue
					}
					bind[name] = nil
					continue
				}
				val := field.Elem().Uint()
				valString := fmt.Sprintf("%04d", val)
				if hasOld && old == valString {
					continue
				}
				bind[name] = valString
				continue
			}
			isNil := field.IsZero()
			if isNil {
				if hasOld && (old == "nil" || old == nil) {
					continue
				}
				bind[name] = nil
				continue
			}
			val := strconv.FormatUint(field.Elem().Uint(), 10)
			if hasOld && old == val {
				continue
			}
			bind[name] = val
		case "int", "int8", "int16", "int32", "int64":
			val := strconv.FormatInt(field.Int(), 10)
			if hasOld && old == val {
				continue
			}
			bind[name] = val
		case "*int", "*int8", "*int16", "*int32", "*int64":
			isNil := field.IsZero()
			if isNil {
				if hasOld && (old == "nil" || old == nil) {
					continue
				}
				bind[name] = nil
				continue
			}
			val := strconv.FormatInt(field.Elem().Int(), 10)
			if hasOld && old == val {
				continue
			}
			bind[name] = val
		case "string":
			value := field.String()
			if hasOld && (old == value || ((old == "nil" || old == nil) && value == "")) {
				continue
			}
			if isRequired || value != "" {
				bind[name] = value
			} else if value == "" {
				bind[name] = nil
			}
		case "[]uint8":
			value := field.Bytes()
			valueAsString := string(value)
			if hasOld && (old == valueAsString || ((old == "nil" || old == nil) && valueAsString == "")) {
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
		case "*bool":
			if field.IsZero() {
				if hasOld && (old == "nil" || old == nil) {
					continue
				}
				bind[name] = nil
				continue
			}
			value := "0"
			if field.Elem().Bool() {
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
				userPrecision, _ := strconv.Atoi(precisionAttribute)
				precision = userPrecision
			}
			valString := strconv.FormatFloat(val, 'g', precision, bitSize)
			decimal, has := attributes["decimal"]
			if has {
				decimalArgs := strings.Split(decimal, ",")
				size, _ := strconv.ParseFloat(decimalArgs[1], 64)
				sizeNumber := math.Pow(10, size)
				val2 := math.Round(val*sizeNumber) / sizeNumber
				valString = fmt.Sprintf("%."+decimalArgs[1]+"f", val2)
			}
			if hasOld && old == valString {
				continue
			}
			bind[name] = valString
		case "*float32", "*float64":
			var val float64
			isZero := field.IsZero()
			if !isZero {
				val = field.Elem().Float()
			}
			precision := 8
			bitSize := 32
			if field.Type().String() == "*float64" {
				bitSize = 64
				precision = 16
			}
			fieldAttributes := tableSchema.tags[name]
			precisionAttribute, has := fieldAttributes["precision"]
			if has {
				userPrecision, _ := strconv.Atoi(precisionAttribute)
				precision = userPrecision
			}
			valString := strconv.FormatFloat(val, 'g', precision, bitSize)
			decimal, has := attributes["decimal"]
			if has {
				decimalArgs := strings.Split(decimal, ",")
				size, _ := strconv.ParseFloat(decimalArgs[1], 64)
				sizeNumber := math.Pow(10, size)
				val2 := math.Round(val*sizeNumber) / sizeNumber
				valString = fmt.Sprintf("%."+decimalArgs[1]+"f", val2)
			}
			if hasOld {
				if isZero {
					if old == "nil" || old == nil {
						continue
					}
				} else if old == valString {
					continue
				}
			}
			if isZero {
				bind[name] = nil
			} else {
				bind[name] = valString
			}
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
			if hasOld && (old == valueAsString || (valueAsString == "" && (old == nil || old == "nil"))) {
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
			if hasOld && (old == valueAsString || (valueAsString == "" && (old == nil || old == "nil"))) {
				continue
			}
			if isRequired || valueAsString != "" {
				bind[name] = valueAsString
			} else if valueAsString == "" {
				bind[name] = nil
			}
		default:
			k := field.Kind().String()
			if k == "struct" {
				subBind := createBind(0, tableSchema, field.Type(), reflect.ValueOf(field.Interface()), oldData, fieldType.Name)
				for key, value := range subBind {
					bind[key] = value
				}
				continue
			} else if k == "ptr" {
				valueAsString := ""
				if !field.IsNil() {
					valueAsString = strconv.FormatUint(field.Elem().Field(1).Uint(), 10)
				}
				if hasOld && (old == valueAsString || ((old == "nil" || old == nil || old == "0") && valueAsString == "")) {
					continue
				}
				if valueAsString == "" || valueAsString == "0" {
					bind[name] = nil
				} else {
					bind[name] = valueAsString
				}
				continue
			} else {
				value := field.Interface()
				var valString string
				if !field.IsZero() {
					if fieldTypeString[0:3] == "[]*" {
						length := field.Len()
						if length > 0 {
							ids := make([]uint64, length)
							for i := 0; i < length; i++ {
								ids[i] = field.Index(i).Interface().(Entity).GetID()
							}
							encoded, _ := jsoniter.ConfigFastest.Marshal(ids)
							valString = string(encoded)
						}
						if hasOld && (old == valString || ((old == "nil" || old == nil || old == "0") && valString == "")) {
							continue
						}
						if valString == "" {
							bind[name] = nil
						} else {
							bind[name] = valString
						}
						continue
					} else {
						var encoded []byte
						if hasOld && old != nil && old != "" {
							oldMap := reflect.New(field.Type()).Interface()
							newMap := reflect.New(field.Type()).Interface()
							_ = jsoniter.ConfigFastest.Unmarshal([]byte(old.(string)), oldMap)
							oldValue := reflect.ValueOf(oldMap).Elem().Interface()
							encoded, _ = jsoniter.ConfigFastest.Marshal(value)
							_ = jsoniter.ConfigFastest.Unmarshal(encoded, newMap)
							newValue := reflect.ValueOf(newMap).Elem().Interface()
							if cmp.Equal(newValue, oldValue) {
								continue
							}
						} else {
							encoded, _ = jsoniter.ConfigFastest.Marshal(value)
						}
						valString = string(encoded)
					}
				} else if hasOld && (old == "nil" || old == nil) {
					continue
				}
				if isRequired || valString != "" {
					bind[name] = valString
				} else if valString == "" {
					bind[name] = nil
				}
			}
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
		if addedDeleted && len(definition.TrackedFields) == 0 {
			keys = append(keys, getCacheKeySearch(schema, indexName))
		}
		for _, trackedField := range definition.TrackedFields {
			_, has := bind[trackedField]
			if has {
				attributes := make([]interface{}, 0)
				for _, trackedFieldSub := range definition.QueryFields {
					val := data[trackedFieldSub]
					if !schema.hasFakeDelete || trackedFieldSub != "FakeDelete" {
						attributes = append(attributes, val)
					}
				}
				keys = append(keys, getCacheKeySearch(schema, indexName, attributes...))
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

func addToDataLoader(values map[*tableSchema]map[uint64][]string, schema *tableSchema, id uint64, value []string) {
	if values[schema] == nil {
		values[schema] = make(map[uint64][]string)
	}
	values[schema][id] = value
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

func addDirtyQueues(keys map[string][]EventAsMap, bind map[string]interface{}, schema *tableSchema, id uint64, action string) {
	results := make(map[string]EventAsMap)
	key := EventAsMap{"E": schema.t.String(), "I": id, "A": action}
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
			results[queueName] = key
		}
	}
	for k, v := range results {
		keys[k] = append(keys[k], v)
	}
}

func addToLogQueue(keys []*LogQueueValue, tableSchema *tableSchema, id uint64,
	before map[string]interface{}, changes map[string]interface{}, entityMeta map[string]interface{}) []*LogQueueValue {
	if !tableSchema.hasLog {
		return keys
	}
	if changes != nil && len(tableSchema.skipLogs) > 0 {
		skipped := 0
		for _, skip := range tableSchema.skipLogs {
			_, has := changes[skip]
			if has {
				skipped++
			}
		}
		if skipped == len(changes) {
			return keys
		}
	}
	val := &LogQueueValue{TableName: tableSchema.logTableName, ID: id,
		PoolName: tableSchema.logPoolName, Before: before,
		Changes: changes, Updated: time.Now(), Meta: entityMeta}
	keys = append(keys, val)
	return keys
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
	sqlErr, yes := errors.Cause(err).(*mysql.MySQLError)
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
				return &ForeignKeyError{Message: fmt.Sprintf("foreign key error in key `%s`", labels[1]), Constraint: labels[1]}
			}
		}
	}
	return err
}

func updateCacheForInserted(entity Entity, lazy bool, id uint64,
	bind map[string]interface{}, localCacheSets map[string]map[string][]interface{}, localCacheDeletes map[string]map[string]bool,
	redisKeysToDelete map[string]map[string]bool, dirtyChannels map[string][]EventAsMap,
	logQueues []*LogQueueValue, dataLoaderSets dataLoaderSets) []*LogQueueValue {
	schema := entity.getORM().tableSchema
	engine := entity.getORM().engine
	localCache, hasLocalCache := schema.GetLocalCache(engine)
	if !hasLocalCache && engine.hasRequestCache {
		hasLocalCache = true
		localCache = engine.GetLocalCache(requestCacheKey)
	}
	redisCache, hasRedis := schema.GetRedisCache(engine)
	if hasLocalCache {
		if !lazy {
			addLocalCacheSet(localCacheSets, schema.GetMysql(engine).GetPoolCode(), localCache.code, schema.getCacheKey(id), buildLocalCacheValue(entity))
		} else {
			addCacheDeletes(localCacheDeletes, localCache.code, schema.getCacheKey(id))
		}
		keys := getCacheQueriesKeys(schema, bind, bind, true)
		addCacheDeletes(localCacheDeletes, localCache.code, keys...)
	} else if !lazy && engine.dataLoader != nil {
		addToDataLoader(dataLoaderSets, schema, id, buildLocalCacheValue(entity))
	}
	if hasRedis {
		addCacheDeletes(redisKeysToDelete, redisCache.code, schema.getCacheKey(id))
		keys := getCacheQueriesKeys(schema, bind, bind, true)
		addCacheDeletes(redisKeysToDelete, redisCache.code, keys...)
	}
	addDirtyQueues(dirtyChannels, bind, schema, id, "i")
	logQueues = addToLogQueue(logQueues, schema, id, nil, bind, entity.getORM().attributes.logMeta)
	return logQueues
}

func getDirtyBind(entity Entity) (is bool, bind map[string]interface{}) {
	orm := entity.getORM()
	if orm.attributes.delete {
		return true, nil
	}
	id := orm.GetID()
	t := orm.attributes.elem.Type()
	bind = createBind(id, orm.tableSchema, t, orm.attributes.elem, orm.dBData, "")
	is = id == 0 || len(bind) > 0
	return is, bind
}

func (e *Engine) flushTrackedEntities(lazy bool, transaction bool, smart bool) {
	if e.trackedEntitiesCounter == 0 {
		return
	}
	var dbPools map[string]*DB
	if transaction {
		dbPools = make(map[string]*DB)
		for _, entity := range e.trackedEntities {
			db := entity.getORM().tableSchema.GetMysql(e)
			dbPools[db.code] = db
		}
		for _, db := range dbPools {
			db.Begin()
		}
	}
	defer func() {
		for _, db := range dbPools {
			db.Rollback()
		}
	}()
	flush(e, lazy, transaction, smart, e.trackedEntities...)
	if transaction {
		for _, db := range dbPools {
			db.Commit()
		}
	}
	e.trackedEntities = nil
	e.trackedEntitiesCounter = 0
}

func (e *Engine) flushWithLock(transaction bool, lockerPool string, lockName string, ttl time.Duration, waitTimeout time.Duration) {
	locker := e.GetLocker(lockerPool)
	lock, has := locker.Obtain(e.context, lockName, ttl, waitTimeout)
	if !has {
		panic(errors.Timeoutf("lock wait"))
	}
	defer lock.Release()
	e.flushTrackedEntities(false, transaction, false)
}

func (e *Engine) flushWithCheck(transaction bool) error {
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				e.ClearTrackedEntities()
				asErr := r.(error)
				source := errors.Cause(asErr)
				assErr1, is := source.(*ForeignKeyError)
				if is {
					err = assErr1
					return
				}
				assErr2, is := source.(*DuplicatedKeyError)
				if is {
					err = assErr2
					return
				}
				panic(asErr)
			}
		}()
		e.flushTrackedEntities(false, transaction, false)
	}()
	return err
}

func addElementsToDirtyQueues(engine *Engine, dirtyChannels map[string][]EventAsMap) {
	broker := engine.GetEventBroker()
	for code, v := range dirtyChannels {
		for _, k := range v {
			broker.PublishMap(code, k)
		}
	}
}

func addElementsToLogQueues(engine *Engine, logQueues []*LogQueueValue) {
	broker := engine.GetEventBroker()
	for _, val := range logQueues {
		if val.Meta == nil {
			val.Meta = engine.logMetaData
		} else {
			for k, v := range engine.logMetaData {
				val.Meta[k] = v
			}
		}
		broker.Publish(logChannelName, val)
	}
}
