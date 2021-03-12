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

	jsoniter "github.com/json-iterator/go"

	"github.com/go-sql-driver/mysql"
)

type Bind map[string]interface{}

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

type dataLoaderSets map[*tableSchema]map[uint64][]interface{}

func flush(engine *Engine, lazy bool, transaction bool, smart bool, entities ...Entity) {
	insertKeys := make(map[reflect.Type][]string)
	insertValues := make(map[reflect.Type]string)
	updateSQLs := make(map[string][]string)
	insertArguments := make(map[reflect.Type][]interface{})
	insertBinds := make(map[reflect.Type][]map[string]interface{})
	insertReflectValues := make(map[reflect.Type][]Entity)
	deleteBinds := make(map[reflect.Type]map[uint64][]interface{})
	totalInsert := make(map[reflect.Type]int)
	localCacheSets := make(map[string]map[string][]interface{})
	dataLoaderSets := make(map[*tableSchema]map[uint64][]interface{})
	localCacheDeletes := make(map[string]map[string]bool)
	lazyMap := make(map[string]interface{})
	rFlusher := engine.afterCommitRedisFlusher
	if rFlusher == nil {
		rFlusher = &redisFlusher{engine: engine}
	}
	isInTransaction := transaction

	var referencesToFlash map[Entity]Entity

	for _, entity := range entities {
		initIfNeeded(engine, entity).initDBData()
		schema := entity.getORM().tableSchema
		if !isInTransaction && schema.GetMysql(engine).inTransaction {
			isInTransaction = true
		}
		for _, refName := range schema.refOne {
			refValue := entity.getORM().elem.FieldByName(refName)
			if refValue.IsValid() && !refValue.IsNil() {
				refEntity := refValue.Interface().(Entity)
				initIfNeeded(engine, refEntity).initDBData()
				if refEntity.GetID() == 0 {
					if referencesToFlash == nil {
						referencesToFlash = make(map[Entity]Entity)
					}
					referencesToFlash[refEntity] = refEntity
				}
			}
		}
		for _, refName := range schema.refMany {
			refValue := entity.getORM().elem.FieldByName(refName)
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
		bind, updateBind, isDirty := orm.getDirtyBind()
		if !isDirty {
			continue
		}
		bindLength := len(bind)

		t := orm.tableSchema.t
		currentID := entity.GetID()
		if orm.fakeDelete && !orm.tableSchema.hasFakeDelete {
			orm.delete = true
		}
		if orm.delete {
			if deleteBinds[t] == nil {
				deleteBinds[t] = make(map[uint64][]interface{})
			}
			deleteBinds[t][currentID] = dbData
		} else if !orm.inDB {
			onUpdate := entity.getORM().onDuplicateKeyUpdate
			if onUpdate != nil {
				if lazy {
					panic(fmt.Errorf("lazy flush on duplicate key is not supported"))
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
					columns[i] = "`" + key + "`"
					values[i] = "?"
					bindRow[i] = val
					i++
				}
				/* #nosec */
				sql := "INSERT INTO " + schema.tableName + "(" + strings.Join(columns, ",") + ") VALUES (" + strings.Join(values, ",") + ")"
				sql += " ON DUPLICATE KEY UPDATE "
				first := true
				for k, v := range onUpdate {
					if !first {
						sql += ", "
					}
					sql += "`" + k + "` = ?"
					bindRow = append(bindRow, v)
					first = false
				}
				if len(onUpdate) == 0 {
					sql += "`Id` = `Id`"
				}
				db := schema.GetMysql(engine)
				result := db.Exec(sql, bindRow...)
				affected := result.RowsAffected()
				if affected > 0 {
					lastID := result.LastInsertId()
					injectBind(entity, bind)
					orm := entity.getORM()
					orm.idElem.SetUint(lastID)
					orm.dBData[0] = lastID
					if affected == 1 {
						updateCacheForInserted(engine, entity, lazy, lastID, bind, localCacheSets, localCacheDeletes,
							rFlusher, dataLoaderSets)
					} else {
						for k, v := range onUpdate {
							err := entity.SetField(k, v)
							checkError(err)
						}
						bind, _ := orm.GetDirtyBind()
						_, _, _ = loadByID(engine, lastID, entity, true, false)
						updateCacheAfterUpdate(lazy, dbData, engine, entity, bind, schema, localCacheSets, localCacheDeletes, db, lastID,
							rFlusher, dataLoaderSets)
					}
				} else {
				OUTER:
					for _, index := range schema.uniqueIndices {
						fields := make([]string, 0)
						binds := make([]interface{}, 0)
						for _, column := range index {
							if bind[column] == nil {
								continue OUTER
							}
							fields = append(fields, "`"+column+"` = ?")
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
				insertValues[t] = "(" + strings.Join(valuesKeys, ",") + ")"
			}
			insertArguments[t] = append(insertArguments[t], values...)
			insertReflectValues[t] = append(insertReflectValues[t], entity)
			insertBinds[t] = append(insertBinds[t], bind)
			totalInsert[t]++
		} else {
			if !entity.Loaded() {
				panic(fmt.Errorf("entity is not loaded and can't be updated: %v [%d]", entity.getORM().elem.Type().String(), currentID))
			}
			fields := make([]string, bindLength)
			i := 0
			for key, value := range updateBind {
				fields[i] = "`" + key + "`=" + value
				i++
			}
			/* #nosec */
			sql := "UPDATE " + schema.GetTableName() + " SET " + strings.Join(fields, ",") + " WHERE `ID` = " + strconv.FormatUint(currentID, 10)
			db := schema.GetMysql(engine)
			if lazy {
				fillLazyQuery(lazyMap, db.GetPoolCode(), sql, nil)
			} else {
				smartUpdate := false
				if smart && !db.inTransaction && schema.hasLocalCache && !schema.hasRedisCache {
					keys := getCacheQueriesKeys(schema, bind, dbData, false)
					smartUpdate = len(keys) == 0
				}
				if smartUpdate {
					fillLazyQuery(lazyMap, db.GetPoolCode(), sql, nil)
				} else {
					updateSQLs[schema.mysqlPoolName] = append(updateSQLs[schema.mysqlPoolName], sql)
				}
			}
			updateCacheAfterUpdate(lazy, dbData, engine, entity, bind, schema, localCacheSets, localCacheDeletes, db, currentID,
				rFlusher, dataLoaderSets)
		}
	}

	if referencesToFlash != nil {
		if lazy {
			panic(fmt.Errorf("lazy flush for unsaved references is not supported"))
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
			finalValues[key] = "`" + val + "`"
		}
		/* #nosec */
		sql := "INSERT INTO " + schema.tableName + "(" + strings.Join(finalValues, ",") + ") VALUES " + insertValues[typeOf]
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
				orm := entity.getORM()
				orm.idElem.SetUint(id)
				orm.dBData[0] = id
				insertedID = id
				id = id + db.autoincrement
			}
			updateCacheForInserted(engine, entity, lazy, insertedID, bind, localCacheSets, localCacheDeletes,
				rFlusher, dataLoaderSets)
		}
	}
	for pool, queries := range updateSQLs {
		db := engine.GetMysql(pool)
		if len(queries) == 1 {
			db.Exec(queries[0])
			continue
		}
		_, def := db.Query(strings.Join(queries, ";") + ";")
		def()
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
		sql := "DELETE FROM `" + schema.tableName + "` WHERE " + NewWhere("`ID` IN ?", ids).String()
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
							where := NewWhere("`"+refColumn+"` IN ?", ids)
							for {
								engine.Search(where, pager, sub)
								total := subElem.Len()
								if total == 0 {
									break
								}
								toDeleteAll := make([]Entity, total)
								for i := 0; i < total; i++ {
									toDeleteValue := subElem.Index(i).Interface().(Entity)
									toDeleteValue.markToDelete()
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
		for id, dbData := range deleteBinds {
			bind := convertDBDataToMap(schema, dbData)
			addDirtyQueues(rFlusher, bind, schema, id, "d")
			addToLogQueue(engine, rFlusher, schema, id, bind, nil, nil)
			if hasLocalCache {
				addLocalCacheSet(localCacheSets, db.GetPoolCode(), localCache.code, schema.getCacheKey(id), "nil")
				keys := getCacheQueriesKeys(schema, bind, dbData, true)
				addLocalCacheDeletes(localCacheDeletes, localCache.code, keys...)
			} else if engine.dataLoader != nil {
				addToDataLoader(dataLoaderSets, schema, id, nil)
			}
			if hasRedis {
				rFlusher.Del(redisCache.code, schema.getCacheKey(id))
				keys := getCacheQueriesKeys(schema, bind, dbData, true)
				rFlusher.Del(redisCache.code, keys...)
			}
			if schema.hasSearchCache {
				key := schema.redisSearchPrefix + strconv.FormatUint(id, 10)
				rFlusher.Del(schema.searchCacheName, key)
			}
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
	if lazy {
		deletesRedisCache, has := lazyMap["cr"].(map[string][]string)
		if !has {
			deletesRedisCache = make(map[string][]string)
			lazyMap["cr"] = deletesRedisCache
		}
		for cacheCode, commands := range rFlusher.pipelines {
			if commands.deletes != nil {
				deletesRedisCache[cacheCode] = commands.deletes
			}
		}
	} else if isInTransaction {
		engine.afterCommitRedisFlusher = rFlusher
	}
	for schema, rows := range dataLoaderSets {
		if !isInTransaction {
			for id, value := range rows {
				engine.dataLoader.Prime(schema, id, value)
			}
		} else {
			if engine.afterCommitDataLoaderSets == nil {
				engine.afterCommitDataLoaderSets = make(map[*tableSchema]map[uint64][]interface{})
			}
			if engine.afterCommitDataLoaderSets[schema] == nil {
				engine.afterCommitDataLoaderSets[schema] = make(map[uint64][]interface{})
			}
			for id, value := range rows {
				engine.afterCommitDataLoaderSets[schema][id] = value
			}
		}
	}
	if len(lazyMap) > 0 {
		rFlusher.Publish(lazyChannelName, lazyMap)
	}
	if !isInTransaction {
		rFlusher.Flush()
	}
}

func updateCacheAfterUpdate(lazy bool, dbData []interface{}, engine *Engine, entity Entity, bind map[string]interface{},
	schema *tableSchema, localCacheSets map[string]map[string][]interface{}, localCacheDeletes map[string]map[string]bool,
	db *DB, currentID uint64, redisFlusher RedisFlusher, dataLoaderSets dataLoaderSets) {
	old := make([]interface{}, len(dbData))
	copy(old, dbData)
	injectBind(entity, bind)
	localCache, hasLocalCache := schema.GetLocalCache(engine)
	if !hasLocalCache && engine.hasRequestCache {
		hasLocalCache = true
		localCache = engine.GetLocalCache(requestCacheKey)
	}
	redisCache, hasRedis := schema.GetRedisCache(engine)
	if hasLocalCache {
		cacheKey := schema.getCacheKey(currentID)
		addLocalCacheSet(localCacheSets, db.GetPoolCode(), localCache.code, cacheKey, buildLocalCacheValue(entity))
		if lazy {
			addLocalCacheDeletes(localCacheDeletes, localCache.code, cacheKey)
		}
		keys := getCacheQueriesKeys(schema, bind, dbData, false)
		addLocalCacheDeletes(localCacheDeletes, localCache.code, keys...)
		keys = getCacheQueriesKeys(schema, bind, old, false)
		addLocalCacheDeletes(localCacheDeletes, localCache.code, keys...)
	} else if engine.dataLoader != nil {
		addToDataLoader(dataLoaderSets, schema, currentID, buildLocalCacheValue(entity))
	}
	if hasRedis {
		redisFlusher.Del(redisCache.code, schema.getCacheKey(currentID))
		keys := getCacheQueriesKeys(schema, bind, dbData, false)
		redisFlusher.Del(redisCache.code, keys...)
		keys = getCacheQueriesKeys(schema, bind, old, false)
		redisFlusher.Del(redisCache.code, keys...)
	}
	fillRedisSearchFromBind(schema, redisFlusher, bind, entity.GetID())
	addDirtyQueues(redisFlusher, bind, schema, currentID, "u")
	addToLogQueue(engine, redisFlusher, schema, currentID, convertDBDataToMap(schema, old), bind, entity.getORM().logMeta)
}

func convertDBDataToMap(schema *tableSchema, data []interface{}) map[string]interface{} {
	m := make(map[string]interface{})
	for _, name := range schema.columnNames[1:] {
		m[name] = data[schema.columnMapping[name]]
	}
	return m
}

func injectBind(entity Entity, bind map[string]interface{}) {
	orm := entity.getORM()
	mapping := orm.tableSchema.columnMapping
	orm.initDBData()
	for key, value := range bind {
		orm.dBData[mapping[key]] = value
	}
	orm.loaded = true
	orm.inDB = true
}

func fillBind(id uint64, bind Bind, updateBind map[string]string, orm *ORM, tableSchema *tableSchema,
	t reflect.Type, value reflect.Value,
	oldData []interface{}, prefix string) {
	var hasOld = orm.inDB
	hasUpdate := updateBind != nil
	for i := 0; i < t.NumField(); i++ {
		fieldType := t.Field(i)
		name := prefix + fieldType.Name
		if prefix == "" && i <= 1 {
			continue
		}
		var old interface{}
		if hasOld {
			old = oldData[tableSchema.columnMapping[name]]
		}
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
			if attributes["year"] == "true" {
				if hasOld && old == val {
					continue
				}
				bind[name] = val
				continue
			}
			if hasOld && old == val {
				continue
			}
			bind[name] = val
			if hasUpdate {
				updateBind[name] = strconv.FormatUint(val, 10)
			}
		case "*uint", "*uint8", "*uint16", "*uint32", "*uint64":
			if attributes["year"] == "true" {
				isNil := field.IsZero()
				if isNil {
					if hasOld && old == nil {
						continue
					}
					bind[name] = nil
					if hasUpdate {
						updateBind[name] = "NULL"
					}
					continue
				}
				val := field.Elem().Uint()
				if hasOld && old == val {
					continue
				}
				bind[name] = val
				if hasUpdate {
					updateBind[name] = strconv.FormatUint(val, 10)
				}
				continue
			}
			isNil := field.IsZero()
			if isNil {
				if hasOld && old == nil {
					continue
				}
				bind[name] = nil
				if hasUpdate {
					updateBind[name] = "NULL"
				}
				continue
			}
			val := field.Elem().Uint()
			if hasOld && old == val {
				continue
			}
			bind[name] = val
			if hasUpdate {
				updateBind[name] = strconv.FormatUint(val, 10)
			}
		case "int", "int8", "int16", "int32", "int64":
			val := field.Int()
			if hasOld && old == val {
				continue
			}
			bind[name] = val
			if hasUpdate {
				updateBind[name] = strconv.FormatInt(val, 10)
			}
		case "*int", "*int8", "*int16", "*int32", "*int64":
			isNil := field.IsZero()
			if isNil {
				if hasOld && old == nil {
					continue
				}
				bind[name] = nil
				if hasUpdate {
					updateBind[name] = "NULL"
				}
				continue
			}
			val := field.Elem().Int()
			if hasOld && old == val {
				continue
			}
			bind[name] = val
			if hasUpdate {
				updateBind[name] = strconv.FormatInt(val, 10)
			}
		case "string":
			value := field.String()
			if hasOld && (old == value || (old == nil && value == "")) {
				continue
			}
			if isRequired || value != "" {
				bind[name] = value
				if hasUpdate {
					updateBind[name] = escapeSQLParam(value)
				}
			} else if value == "" {
				bind[name] = nil
				if hasUpdate {
					updateBind[name] = "NULL"
				}
			}
		case "[]uint8":
			value := field.Bytes()
			valueAsString := string(value)
			if hasOld && ((old != nil && old.(string) == valueAsString) || (old == nil && valueAsString == "")) {
				continue
			}
			if valueAsString == "" {
				bind[name] = nil
				if hasUpdate {
					updateBind[name] = "NULL"
				}
			} else {
				bind[name] = valueAsString
				if hasUpdate {
					updateBind[name] = escapeSQLParam(valueAsString)
				}
			}
		case "bool":
			if name == "FakeDelete" {
				value := uint64(0)
				if field.Bool() {
					value = id
				}
				if hasOld && old == value {
					continue
				}
				bind[name] = value
				if hasUpdate {
					updateBind[name] = strconv.FormatUint(value, 10)
				}
				continue
			}
			value := field.Bool()
			if hasOld && old == value {
				continue
			}
			bind[name] = value
			if hasUpdate {
				if value {
					updateBind[name] = "1"
				} else {
					updateBind[name] = "0"
				}
			}
		case "*bool":
			if field.IsZero() {
				if hasOld && old == nil {
					continue
				}
				bind[name] = nil
				if hasUpdate {
					updateBind[name] = "NULL"
				}
				continue
			}
			value := field.Elem().Bool()
			if hasOld && old == value {
				continue
			}
			bind[name] = value
			if hasUpdate {
				if value {
					updateBind[name] = "1"
				} else {
					updateBind[name] = "0"
				}
			}
		case "float32", "float64":
			val := field.Float()
			precision := 8
			if field.Type().String() == "float64" {
				precision = 16
			}
			fieldAttributes := tableSchema.tags[name]
			precisionAttribute, has := fieldAttributes["precision"]
			if has {
				userPrecision, _ := strconv.Atoi(precisionAttribute)
				precision = userPrecision
			}
			decimal, has := attributes["decimal"]
			if has {
				decimalArgs := strings.Split(decimal, ",")
				size, _ := strconv.ParseFloat(decimalArgs[1], 64)
				sizeNumber := math.Pow(10, size)
				val = math.Round(val*sizeNumber) / sizeNumber
				if hasOld {
					valOld := math.Round(old.(float64)*sizeNumber) / sizeNumber
					if val == valOld {
						continue
					}
				}
			} else {
				sizeNumber := math.Pow(10, float64(precision))
				val = math.Round(val*sizeNumber) / sizeNumber
				if hasOld {
					valOld := math.Round(old.(float64)*sizeNumber) / sizeNumber
					if valOld == val {
						continue
					}
				}
			}
			bind[name] = val
			if hasUpdate {
				updateBind[name] = strconv.FormatFloat(val, 'f', -1, 64)
			}
		case "*float32", "*float64":
			var val float64
			isZero := field.IsZero()
			if !isZero {
				val = field.Elem().Float()
			}
			precision := 5
			if field.Type().String() == "*float64" {
				precision = 10
			}
			fieldAttributes := tableSchema.tags[name]
			precisionAttribute, has := fieldAttributes["precision"]
			if has {
				userPrecision, _ := strconv.Atoi(precisionAttribute)
				precision = userPrecision
			}
			decimal, has := attributes["decimal"]
			if has {
				if isZero {
					if hasOld && old == nil {
						continue
					}
					bind[name] = nil
					if hasUpdate {
						updateBind[name] = "NULL"
					}
					continue
				}
				decimalArgs := strings.Split(decimal, ",")
				size, _ := strconv.ParseFloat(decimalArgs[1], 64)
				sizeNumber := math.Pow(10, size)
				val = math.Round(val*sizeNumber) / sizeNumber
				if hasOld && old != nil {
					valOld := math.Round(old.(float64)*sizeNumber) / sizeNumber
					if val == valOld {
						continue
					}
				}
				bind[name] = val
				if hasUpdate {
					updateBind[name] = strconv.FormatFloat(val, 'f', -1, 64)
				}
			} else {
				if isZero {
					if hasOld && old == nil {
						continue
					}
					bind[name] = nil
					if hasUpdate {
						updateBind[name] = "NULL"
					}
					continue
				}
				sizeNumber := math.Pow(10, float64(precision))
				val = math.Round(val*sizeNumber) / sizeNumber
				if hasOld && old != nil {
					valOld := math.Round(old.(float64)*sizeNumber) / sizeNumber
					if valOld == val {
						continue
					}
				}
				bind[name] = val
				if hasUpdate {
					updateBind[name] = strconv.FormatFloat(val, 'f', -1, 64)
				}
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
			if hasUpdate {
				updateBind[name] = "'" + valueAsString + "'"
			}
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
				if hasUpdate {
					updateBind[name] = "NULL"
				}
			} else {
				bind[name] = valueAsString
				if hasUpdate {
					updateBind[name] = "'" + valueAsString + "'"
				}
			}
		case "[]string":
			value := field.Interface().([]string)
			var valueAsString string
			if value != nil {
				valueAsString = strings.Join(value, ",")
			}
			if hasOld && (old == valueAsString || (valueAsString == "" && old == nil)) {
				continue
			}
			if isRequired || valueAsString != "" {
				bind[name] = valueAsString
				if hasUpdate {
					updateBind[name] = escapeSQLParam(valueAsString)
				}
			} else if valueAsString == "" {
				bind[name] = nil
				if hasUpdate {
					updateBind[name] = "NULL"
				}
			}
		default:
			k := field.Kind().String()
			if k == "struct" {
				fillBind(0, bind, updateBind, orm, tableSchema, field.Type(), reflect.ValueOf(field.Interface()), oldData, fieldType.Name)
				continue
			} else if k == "ptr" {
				value := uint64(0)
				if !field.IsNil() {
					value = field.Elem().Field(1).Uint()
				}
				if hasOld && (old == value || ((old == nil || old == 0) && value == 0)) {
					continue
				}
				if value == 0 {
					bind[name] = nil
					if hasUpdate {
						updateBind[name] = "NULL"
					}
				} else {
					bind[name] = value
					if hasUpdate {
						updateBind[name] = strconv.FormatUint(value, 10)
					}
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
						if hasOld && (old == valString || ((old == nil || old == "0") && valString == "")) {
							continue
						}
						if valString == "" {
							bind[name] = nil
							if hasUpdate {
								updateBind[name] = "NULL"
							}
						} else {
							bind[name] = valString
							if hasUpdate {
								updateBind[name] = "'" + valString + "'"
							}
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
				} else if hasOld && old == nil {
					continue
				}
				if isRequired || valString != "" {
					bind[name] = valString
					if hasUpdate {
						updateBind[name] = "'" + valString + "'"
					}
				} else if valString == "" {
					bind[name] = nil
					if hasUpdate {
						updateBind[name] = "NULL"
					}
				}
			}
		}
	}
}

func escapeSQLParam(val string) string {
	dest := make([]byte, 0, 2*len(val))
	var escape byte
	for i := 0; i < len(val); i++ {
		c := val[i]
		escape = 0
		switch c {
		case 0:
			escape = '0'
		case '\n':
			escape = 'n'
		case '\r':
			escape = 'r'
		case '\\':
			escape = '\\'
		case '\'':
			escape = '\''
		case '"':
			escape = '"'
		case '\032':
			escape = 'Z'
		}
		if escape != 0 {
			dest = append(dest, '\\', escape)
		} else {
			dest = append(dest, c)
		}
	}
	return "'" + string(dest) + "'"
}

func getCacheQueriesKeys(schema *tableSchema, bind map[string]interface{}, data []interface{}, addedDeleted bool) (keys []string) {
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
					val := data[schema.columnMapping[trackedFieldSub]]
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

func addToDataLoader(values map[*tableSchema]map[uint64][]interface{}, schema *tableSchema, id uint64, value []interface{}) {
	if values[schema] == nil {
		values[schema] = make(map[uint64][]interface{})
	}
	values[schema][id] = value
}

func addLocalCacheDeletes(cacheDeletes map[string]map[string]bool, cacheCode string, keys ...string) {
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

func addDirtyQueues(redisFlusher RedisFlusher, bind map[string]interface{}, schema *tableSchema, id uint64, action string) {
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
			redisFlusher.PublishMap(queueName, key)
		}
	}
}

func addToLogQueue(engine *Engine, redisFlusher RedisFlusher, tableSchema *tableSchema, id uint64,
	before map[string]interface{}, changes map[string]interface{}, entityMeta map[string]interface{}) {
	if !tableSchema.hasLog {
		return
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
			return
		}
	}
	val := &LogQueueValue{TableName: tableSchema.logTableName, ID: id,
		PoolName: tableSchema.logPoolName, Before: before,
		Changes: changes, Updated: time.Now(), Meta: entityMeta}
	if val.Meta == nil {
		val.Meta = engine.logMetaData
	} else {
		for k, v := range engine.logMetaData {
			val.Meta[k] = v
		}
	}
	redisFlusher.Publish(logChannelName, val)
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
				return &ForeignKeyError{Message: "foreign key error in key `" + labels[1] + "`", Constraint: labels[1]}
			}
		}
	}
	return err
}

func updateCacheForInserted(engine *Engine, entity Entity, lazy bool, id uint64,
	bind map[string]interface{}, localCacheSets map[string]map[string][]interface{}, localCacheDeletes map[string]map[string]bool,
	redisFlusher RedisFlusher, dataLoaderSets dataLoaderSets) {
	schema := entity.getORM().tableSchema
	localCache, hasLocalCache := schema.GetLocalCache(engine)
	if !hasLocalCache && engine.hasRequestCache {
		hasLocalCache = true
		localCache = engine.GetLocalCache(requestCacheKey)
	}
	if hasLocalCache {
		if !lazy {
			addLocalCacheSet(localCacheSets, schema.GetMysql(engine).GetPoolCode(), localCache.code, schema.getCacheKey(id), buildLocalCacheValue(entity))
		} else {
			addLocalCacheDeletes(localCacheDeletes, localCache.code, schema.getCacheKey(id))
		}
		keys := getCacheQueriesKeys(schema, bind, entity.getORM().dBData, true)
		addLocalCacheDeletes(localCacheDeletes, localCache.code, keys...)
	} else if !lazy && engine.dataLoader != nil {
		addToDataLoader(dataLoaderSets, schema, id, buildLocalCacheValue(entity))
	}
	redisCache, hasRedis := schema.GetRedisCache(engine)
	if hasRedis {
		redisFlusher.Del(redisCache.code, schema.getCacheKey(id))
		keys := getCacheQueriesKeys(schema, bind, entity.getORM().dBData, true)
		redisFlusher.Del(redisCache.code, keys...)
	}
	fillRedisSearchFromBind(schema, redisFlusher, bind, id)

	addDirtyQueues(redisFlusher, bind, schema, id, "i")
	addToLogQueue(engine, redisFlusher, schema, id, nil, bind, entity.getORM().logMeta)
}

func fillRedisSearchFromBind(schema *tableSchema, redisFlusher RedisFlusher, bind map[string]interface{}, id uint64) {
	if schema.hasSearchCache {
		values := make([]interface{}, 0)
		for k, f := range schema.mapBindToRedisSearch {
			v, has := bind[k]
			if has {
				values = append(values, k, f(v))
			}
		}
		if len(values) > 0 {
			key := schema.redisSearchPrefix + strconv.FormatUint(id, 10)
			redisFlusher.HSet(schema.searchCacheName, key, values...)
		}
	}
}
