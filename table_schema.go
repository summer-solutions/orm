package orm

import (
	"crypto/sha256"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type CachedQuery struct{}

type cachedQueryDefinition struct {
	Max           int
	Query         string
	TrackedFields []string
	QueryFields   []string
	OrderFields   []string
}

type Enum interface {
	GetFields() []string
	GetMapping() map[string]string
	GetDefault() string
	Has(value string) bool
	init(ref interface{})
}

type EnumModel struct {
	fields       []string
	mapping      map[string]string
	defaultValue string
}

func (enum *EnumModel) GetFields() []string {
	return enum.fields
}

func (enum *EnumModel) GetMapping() map[string]string {
	return enum.mapping
}

func (enum *EnumModel) GetDefault() string {
	return enum.defaultValue
}

func (enum *EnumModel) Has(value string) bool {
	_, has := enum.mapping[value]
	return has
}

func (enum *EnumModel) init(ref interface{}) {
	e := reflect.ValueOf(ref).Elem()
	enum.mapping = make(map[string]string)
	enum.fields = make([]string, 0)
	for i := 1; i < e.Type().NumField(); i++ {
		name := e.Field(i).String()
		enum.fields = append(enum.fields, name)
		enum.mapping[name] = name
	}
	enum.defaultValue = enum.fields[0]
}

type TableSchema interface {
	GetTableName() string
	GetType() reflect.Type
	DropTable(engine *Engine)
	TruncateTable(engine *Engine)
	UpdateSchema(engine *Engine)
	UpdateSchemaAndTruncateTable(engine *Engine)
	GetMysql(engine *Engine) *DB
	GetLocalCache(engine *Engine) (cache *LocalCache, has bool)
	GetRedisCache(engine *Engine) (cache *RedisCache, has bool)
	GetReferences() []string
	GetColumns() []string
	GetUsage(registry ValidatedRegistry) map[reflect.Type][]string
	GetSchemaChanges(engine *Engine) (has bool, alters []Alter)
}

type tableSchema struct {
	tableName            string
	mysqlPoolName        string
	t                    reflect.Type
	fields               *tableFields
	fieldsQuery          string
	tags                 map[string]map[string]string
	cachedIndexes        map[string]*cachedQueryDefinition
	cachedIndexesOne     map[string]*cachedQueryDefinition
	cachedIndexesAll     map[string]*cachedQueryDefinition
	columnNames          []string
	columnMapping        map[string]int
	uniqueIndices        map[string][]string
	uniqueIndicesGlobal  map[string][]string
	refOne               []string
	refMany              []string
	localCacheName       string
	hasLocalCache        bool
	redisCacheName       string
	hasRedisCache        bool
	searchCacheName      string
	hasSearchCache       bool
	cachePrefix          string
	hasFakeDelete        bool
	hasLog               bool
	logPoolName          string //name of redis
	logTableName         string
	skipLogs             []string
	redisSearchPrefix    string
	redisSearchIndex     *RedisSearchIndex
	mapBindToRedisSearch mapBindToRedisSearch
}

type mapBindToRedisSearch map[string]func(val interface{}) interface{}

type tableFields struct {
	t                 reflect.Type
	fields            map[int]reflect.StructField
	prefix            string
	uintegers         []int
	uintegersNullable []int
	integers          []int
	integersNullable  []int
	strings           []int
	sliceStrings      []int
	bytes             []int
	fakeDelete        int
	booleans          []int
	booleansNullable  []int
	floats            []int
	floatsNullable    []int
	timesNullable     []int
	times             []int
	jsons             []int
	structs           map[int]*tableFields
	refs              []int
	refsTypes         []reflect.Type
	refsMany          []int
	refsManyTypes     []reflect.Type
}

func getTableSchema(registry *validatedRegistry, entityType reflect.Type) *tableSchema {
	return registry.tableSchemas[entityType]
}

func (tableSchema *tableSchema) GetTableName() string {
	return tableSchema.tableName
}

func (tableSchema *tableSchema) GetType() reflect.Type {
	return tableSchema.t
}

func (tableSchema *tableSchema) DropTable(engine *Engine) {
	pool := tableSchema.GetMysql(engine)
	pool.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`;", pool.GetDatabaseName(), tableSchema.tableName))
}

func (tableSchema *tableSchema) TruncateTable(engine *Engine) {
	pool := tableSchema.GetMysql(engine)
	_ = pool.Exec(fmt.Sprintf("DELETE FROM `%s`.`%s`", pool.GetDatabaseName(), tableSchema.tableName))
	_ = pool.Exec(fmt.Sprintf("ALTER TABLE `%s`.`%s` AUTO_INCREMENT = 1", pool.GetDatabaseName(), tableSchema.tableName))
}

func (tableSchema *tableSchema) UpdateSchema(engine *Engine) {
	pool := tableSchema.GetMysql(engine)
	has, alters := tableSchema.GetSchemaChanges(engine)
	if has {
		for _, alter := range alters {
			_ = pool.Exec(alter.SQL)
		}
	}
}

func (tableSchema *tableSchema) UpdateSchemaAndTruncateTable(engine *Engine) {
	tableSchema.UpdateSchema(engine)
	pool := tableSchema.GetMysql(engine)
	_ = pool.Exec(fmt.Sprintf("DELETE FROM `%s`.`%s`", pool.GetDatabaseName(), tableSchema.tableName))
	_ = pool.Exec(fmt.Sprintf("ALTER TABLE `%s`.`%s` AUTO_INCREMENT = 1", pool.GetDatabaseName(), tableSchema.tableName))
}

func (tableSchema *tableSchema) GetMysql(engine *Engine) *DB {
	return engine.GetMysql(tableSchema.mysqlPoolName)
}

func (tableSchema *tableSchema) GetLocalCache(engine *Engine) (cache *LocalCache, has bool) {
	if !tableSchema.hasLocalCache {
		return nil, false
	}
	return engine.GetLocalCache(tableSchema.localCacheName), true
}

func (tableSchema *tableSchema) GetRedisCache(engine *Engine) (cache *RedisCache, has bool) {
	if !tableSchema.hasRedisCache {
		return nil, false
	}
	return engine.GetRedis(tableSchema.redisCacheName), true
}

func (tableSchema *tableSchema) GetRedisSearch(engine *Engine) (search *RedisSearch, has bool) {
	if !tableSchema.hasSearchCache {
		return nil, false
	}
	return engine.GetRedisSearch(tableSchema.searchCacheName), true
}

func (tableSchema *tableSchema) GetReferences() []string {
	return tableSchema.refOne
}

func (tableSchema *tableSchema) GetColumns() []string {
	return tableSchema.columnNames
}

func (tableSchema *tableSchema) GetUsage(registry ValidatedRegistry) map[reflect.Type][]string {
	vRegistry := registry.(*validatedRegistry)
	results := make(map[reflect.Type][]string)
	if vRegistry.entities != nil {
		for _, t := range vRegistry.entities {
			schema := getTableSchema(vRegistry, t)
			for _, columnName := range schema.refOne {
				ref, has := schema.tags[columnName]["ref"]
				if has && ref == tableSchema.t.String() {
					if results[t] == nil {
						results[t] = make([]string, 0)
					}
					results[t] = append(results[t], columnName)
				}
			}
		}
	}
	return results
}

func (tableSchema *tableSchema) GetSchemaChanges(engine *Engine) (has bool, alters []Alter) {
	return getSchemaChanges(engine, tableSchema)
}

func initTableSchema(registry *Registry, entityType reflect.Type) (*tableSchema, error) {
	tags := extractTags(registry, entityType, "")
	oneRefs := make([]string, 0)
	manyRefs := make([]string, 0)
	mapBindToRedisSearch := mapBindToRedisSearch{}
	mysql, has := tags["ORM"]["mysql"]
	if !has {
		mysql = "default"
	}
	_, has = registry.sqlClients[mysql]
	if !has {
		return nil, fmt.Errorf("mysql pool '%s' not found", mysql)
	}
	table, has := tags["ORM"]["table"]
	if !has {
		table = entityType.Name()
	}
	localCache := ""
	redisCache := ""
	redisSearch := ""
	userValue, has := tags["ORM"]["localCache"]
	if has {
		if userValue == "true" {
			userValue = "default"
		}
		localCache = userValue
	}
	if localCache != "" {
		_, has = registry.localCacheContainers[localCache]
		if !has {
			return nil, fmt.Errorf("local cache pool '%s' not found", localCache)
		}
	}
	userValue, has = tags["ORM"]["redisCache"]
	if has {
		if userValue == "true" {
			userValue = "default"
		}
		redisCache = userValue
	}
	if redisCache != "" {
		_, has = registry.redisServers[redisCache]
		if !has {
			return nil, fmt.Errorf("redis pool '%s' not found", redisCache)
		}
	}
	userValue, has = tags["ORM"]["redisSearch"]
	if has {
		if userValue == "true" {
			userValue = "default"
		}
		redisSearch = userValue
	}
	if redisSearch != "" {
		_, has = registry.redisServers[redisSearch]
		if !has {
			return nil, fmt.Errorf("redis pool '%s' not found", redisSearch)
		}
	} else {
		redisSearch = "default"
	}
	cachePrefix := ""
	if mysql != "default" {
		cachePrefix = mysql
	}
	cachePrefix += table
	cachedQueries := make(map[string]*cachedQueryDefinition)
	cachedQueriesOne := make(map[string]*cachedQueryDefinition)
	cachedQueriesAll := make(map[string]*cachedQueryDefinition)
	hasFakeDelete := false
	fakeDeleteField, has := entityType.FieldByName("FakeDelete")
	if has && fakeDeleteField.Type.String() == "bool" {
		hasFakeDelete = true
	}
	for key, values := range tags {
		isOne := false
		query, has := values["query"]
		if !has {
			query, has = values["queryOne"]
			isOne = true
		}
		queryOrigin := query
		fields := make([]string, 0)
		fieldsTracked := make([]string, 0)
		fieldsQuery := make([]string, 0)
		fieldsOrder := make([]string, 0)
		if has {
			re := regexp.MustCompile(":([A-Za-z0-9])+")
			variables := re.FindAllString(query, -1)
			for _, variable := range variables {
				fieldName := variable[1:]
				has := false
				for _, v := range fields {
					if v == fieldName {
						has = true
						break
					}
				}
				if !has {
					fields = append(fields, fieldName)
				}
				query = strings.Replace(query, variable, fmt.Sprintf("`%s`", fieldName), 1)
			}
			if hasFakeDelete && len(variables) > 0 {
				fields = append(fields, "FakeDelete")
			}
			if query == "" {
				if hasFakeDelete {
					query = "`FakeDelete` = 0 ORDER BY `ID`"
				} else {
					query = "1 ORDER BY `ID`"
				}
			} else if hasFakeDelete {
				query = "`FakeDelete` = 0 AND " + query
			}
			queryLower := strings.ToLower(queryOrigin)
			posOrderBy := strings.Index(queryLower, "order by")
			for _, f := range fields {
				if f != "ID" {
					fieldsTracked = append(fieldsTracked, f)
				}
				pos := strings.Index(queryOrigin, ":"+f)
				if pos < posOrderBy || posOrderBy == -1 {
					fieldsQuery = append(fieldsQuery, f)
				}
			}
			if posOrderBy > -1 {
				variables = re.FindAllString(queryOrigin[posOrderBy:], -1)
				for _, variable := range variables {
					fieldName := variable[1:]
					fieldsOrder = append(fieldsOrder, fieldName)
				}
			}

			if !isOne {
				def := &cachedQueryDefinition{50000, query, fieldsTracked, fieldsQuery, fieldsOrder}
				cachedQueries[key] = def
				cachedQueriesAll[key] = def
			} else {
				def := &cachedQueryDefinition{1, query, fieldsTracked, fieldsQuery, fieldsOrder}
				cachedQueriesOne[key] = def
				cachedQueriesAll[key] = def
			}
		}
		_, has = values["ref"]
		if has {
			oneRefs = append(oneRefs, key)
		}
		_, has = values["refs"]
		if has {
			manyRefs = append(manyRefs, key)
		}
	}
	logPoolName := tags["ORM"]["log"]
	if logPoolName == "true" {
		logPoolName = mysql
	}
	uniqueIndices := make(map[string]map[int]string)
	uniqueIndicesSimple := make(map[string][]string)
	uniqueIndicesSimpleGlobal := make(map[string][]string)
	indices := make(map[string]map[int]string)
	skipLogs := make([]string, 0)
	uniqueGlobal, has := tags["ORM"]["unique"]
	if has {
		parts := strings.Split(uniqueGlobal, "|")
		for _, part := range parts {
			def := strings.Split(part, ":")
			uniqueIndices[def[0]] = make(map[int]string)
			uniqueIndicesSimple[def[0]] = make([]string, 0)
			uniqueIndicesSimpleGlobal[def[0]] = make([]string, 0)
			for i, field := range strings.Split(def[1], ",") {
				uniqueIndices[def[0]][i+1] = field
				uniqueIndicesSimple[def[0]] = append(uniqueIndicesSimple[def[0]], field)
				uniqueIndicesSimpleGlobal[def[0]] = append(uniqueIndicesSimpleGlobal[def[0]], field)
			}
		}
	}
	for k, v := range tags {
		keys, has := v["unique"]
		if has && k != "ORM" {
			values := strings.Split(keys, ",")
			for _, indexName := range values {
				parts := strings.Split(indexName, ":")
				id := int64(1)
				if len(parts) > 1 {
					id, _ = strconv.ParseInt(parts[1], 10, 64)
				}
				if uniqueIndices[parts[0]] == nil {
					uniqueIndices[parts[0]] = make(map[int]string)
				}
				uniqueIndices[parts[0]][int(id)] = k
				if uniqueIndicesSimple[parts[0]] == nil {
					uniqueIndicesSimple[parts[0]] = make([]string, 0)
				}
				uniqueIndicesSimple[parts[0]] = append(uniqueIndicesSimple[parts[0]], k)
			}
		}
		keys, has = v["index"]
		if has {
			values := strings.Split(keys, ",")
			for _, indexName := range values {
				parts := strings.Split(indexName, ":")
				id := int64(1)
				if len(parts) > 1 {
					id, _ = strconv.ParseInt(parts[1], 10, 64)
				}
				if indices[parts[0]] == nil {
					indices[parts[0]] = make(map[int]string)
				}
				indices[parts[0]][int(id)] = k
			}
		}
		_, has = v["skip-log"]
		if has {
			skipLogs = append(skipLogs, k)
		}
	}
	for _, ref := range oneRefs {
		has := false
		for _, v := range indices {
			if v[1] == ref {
				has = true
				break
			}
		}
		if !has {
			for _, v := range uniqueIndices {
				if v[1] == ref {
					has = true
					break
				}
			}
			if !has {
				indices["_"+ref] = map[int]string{1: ref}
			}
		}
	}
	redisSearchIndex := &RedisSearchIndex{}
	fields := buildTableFields(entityType, registry, redisSearchIndex, mapBindToRedisSearch, 1, "", tags)
	searchPrefix := ""
	if len(redisSearchIndex.Fields) > 0 {
		redisSearchIndex.Name = entityType.String()
		redisSearchIndex.RedisPool = redisSearch
		searchPrefix = fmt.Sprintf("%x", sha256.Sum256([]byte(entityType.String())))
		searchPrefix = searchPrefix[0:5] + ":"
		redisSearchIndex.Prefixes = []string{searchPrefix}
		redisSearchIndex.NoOffsets = true
		redisSearchIndex.NoFreqs = true
		redisSearchIndex.NoNHL = true
	} else {
		redisSearchIndex = nil
	}
	columns := fields.getColumnNames()
	columnMapping := make(map[string]int)
	for i, name := range columns {
		columnMapping[name] = i
	}
	fieldsQuery := ""
	for _, column := range columns {
		fieldsQuery += ",`" + column + "`"
	}
	cachePrefix = fmt.Sprintf("%x", sha256.Sum256([]byte(cachePrefix+fieldsQuery)))
	cachePrefix = cachePrefix[0:5]
	if redisSearchIndex == nil {
		redisSearch = ""
	}
	tableSchema := &tableSchema{tableName: table,
		mysqlPoolName:        mysql,
		t:                    entityType,
		fields:               fields,
		fieldsQuery:          fieldsQuery[1:],
		redisSearchPrefix:    searchPrefix,
		redisSearchIndex:     redisSearchIndex,
		mapBindToRedisSearch: mapBindToRedisSearch,
		tags:                 tags,
		columnNames:          columns,
		columnMapping:        columnMapping,
		cachedIndexes:        cachedQueries,
		cachedIndexesOne:     cachedQueriesOne,
		cachedIndexesAll:     cachedQueriesAll,
		localCacheName:       localCache,
		hasLocalCache:        localCache != "",
		redisCacheName:       redisCache,
		hasRedisCache:        redisCache != "",
		searchCacheName:      redisSearch,
		hasSearchCache:       redisSearchIndex != nil,
		refOne:               oneRefs,
		refMany:              manyRefs,
		cachePrefix:          cachePrefix,
		uniqueIndices:        uniqueIndicesSimple,
		uniqueIndicesGlobal:  uniqueIndicesSimpleGlobal,
		hasFakeDelete:        hasFakeDelete,
		hasLog:               logPoolName != "",
		logPoolName:          logPoolName,
		logTableName:         fmt.Sprintf("_log_%s_%s", mysql, table),
		skipLogs:             skipLogs}

	all := make(map[string]map[int]string)
	for k, v := range uniqueIndices {
		all[k] = v
	}
	for k, v := range indices {
		all[k] = v
	}
	for k, v := range all {
		for k2, v2 := range all {
			if k == k2 {
				continue
			}
			same := 0
			for i := 1; i <= len(v); i++ {
				right, has := v2[i]
				if has && right == v[i] {
					same++
					continue
				}
				break
			}
			if same == len(v) {
				return nil, fmt.Errorf("duplicated index %s with %s in %s", k, k2, entityType.String())
			}
		}
	}
	for k, v := range tableSchema.cachedIndexesOne {
		ok := false
		for _, columns := range uniqueIndices {
			if len(columns) != len(v.QueryFields) {
				continue
			}
			valid := 0
			for _, field1 := range v.QueryFields {
				for _, field2 := range columns {
					if field1 == field2 {
						valid++
					}
				}
			}
			if valid == len(columns) {
				ok = true
			}
		}
		if !ok {
			return nil, fmt.Errorf("missing unique index for cached query '%s' in %s", k, entityType.String())
		}
	}
	for k, v := range tableSchema.cachedIndexes {
		if v.Query == "1 ORDER BY `ID`" {
			continue
		}
		//first do we have query fields
		ok := false
		for _, columns := range all {
			valid := 0
			for _, field1 := range v.QueryFields {
				for _, field2 := range columns {
					if field1 == field2 {
						valid++
					}
				}
			}
			if valid == len(v.QueryFields) {
				if len(v.OrderFields) == 0 {
					ok = true
					break
				}
				valid := 0
				key := len(columns)
				if columns[len(columns)] == "FakeDelete" {
					key--
				}
				for i := len(v.OrderFields); i > 0; i-- {
					if columns[key] == v.OrderFields[i-1] {
						valid++
						key--
						continue
					}
					break
				}
				if valid == len(v.OrderFields) {
					ok = true
				}
			}
		}
		if !ok {
			return nil, fmt.Errorf("missing index for cached query '%s' in %s", k, entityType.String())
		}
	}
	return tableSchema, nil
}

func buildTableFields(t reflect.Type, registry *Registry, index *RedisSearchIndex, mapBindToRedisSearch mapBindToRedisSearch,
	start int, prefix string, schemaTags map[string]map[string]string) *tableFields {
	fields := &tableFields{t: t, prefix: prefix, uintegers: make([]int, 0), uintegersNullable: make([]int, 0),
		integers: make([]int, 0), integersNullable: make([]int, 0), strings: make([]int, 0), fields: make(map[int]reflect.StructField),
		sliceStrings: make([]int, 0), bytes: make([]int, 0), booleans: make([]int, 0), booleansNullable: make([]int, 0), floats: make([]int, 0),
		timesNullable: make([]int, 0), times: make([]int, 0), jsons: make([]int, 0), structs: make(map[int]*tableFields),
		floatsNullable: make([]int, 0), refs: make([]int, 0), refsTypes: make([]reflect.Type, 0), refsMany: make([]int, 0), refsManyTypes: make([]reflect.Type, 0)}
	for i := start; i < t.NumField(); i++ {
		f := t.Field(i)
		fields.fields[i] = f
		tags := schemaTags[f.Name]
		typeName := f.Type.String()
		_, has := tags["ignore"]
		if has {
			continue
		}
		_, hasSearchable := tags["searchable"]
		_, hasSortable := tags["sortable"]
		switch typeName {
		case "uint",
			"uint8",
			"uint16",
			"uint32",
			"uint64":
			fields.uintegers = append(fields.uintegers, i)
			if hasSearchable || hasSortable {
				index.AddNumericField(prefix+f.Name, hasSortable, !hasSearchable)
				mapBindToRedisSearch[prefix+f.Name] = defaultRedisSearchMapper
			}
		case "*uint",
			"*uint8",
			"*uint16",
			"*uint32",
			"*uint64":
			fields.uintegersNullable = append(fields.uintegersNullable, i)
			if hasSearchable || hasSortable {
				index.AddNumericField(prefix+f.Name, hasSortable, !hasSearchable)
				mapBindToRedisSearch[prefix+f.Name] = defaultRedisSearchMapperNullableInt
			}
		case "int",
			"int8",
			"int16",
			"int32",
			"int64":
			fields.integers = append(fields.integers, i)
			if hasSearchable || hasSortable {
				index.AddNumericField(prefix+f.Name, hasSortable, !hasSearchable)
				mapBindToRedisSearch[prefix+f.Name] = defaultRedisSearchMapper
			}
		case "*int",
			"*int8",
			"*int16",
			"*int32",
			"*int64":
			fields.integersNullable = append(fields.integersNullable, i)
			if hasSearchable || hasSortable {
				index.AddNumericField(prefix+f.Name, hasSortable, !hasSearchable)
				mapBindToRedisSearch[prefix+f.Name] = defaultRedisSearchMapperNullableInt
			}
		case "string":
			fields.strings = append(fields.strings, i)
			if hasSearchable || hasSortable {
				_, hasEnum := tags["enum"]
				if hasEnum {
					index.AddTagField(prefix+f.Name, hasSortable, !hasSearchable, ",")
					mapBindToRedisSearch[prefix+f.Name] = defaultRedisSearchMapperNullableString
				}
			}
		case "[]string":
			fields.sliceStrings = append(fields.sliceStrings, i)
		case "[]uint8":
			fields.bytes = append(fields.bytes, i)
		case "bool":
			if f.Name == "FakeDelete" {
				fields.fakeDelete = i
			} else {
				fields.booleans = append(fields.booleans, i)
			}
		case "*bool":
			fields.booleansNullable = append(fields.booleansNullable, i)
		case "float32",
			"float64":
			fields.floats = append(fields.floats, i)
			if hasSearchable || hasSortable {
				index.AddNumericField(prefix+f.Name, hasSortable, !hasSearchable)
				mapBindToRedisSearch[prefix+f.Name] = defaultRedisSearchMapper
			}
		case "*float32",
			"*float64":
			fields.floatsNullable = append(fields.floatsNullable, i)
		case "*time.Time":
			fields.timesNullable = append(fields.timesNullable, i)
		case "time.Time":
			fields.times = append(fields.times, i)
		default:
			k := f.Type.Kind().String()
			if k == "struct" {
				fields.structs[i] = buildTableFields(f.Type, registry, index, mapBindToRedisSearch, 0, f.Name, schemaTags)
			} else if k == "ptr" {
				modelType := reflect.TypeOf((*Entity)(nil)).Elem()
				if f.Type.Implements(modelType) {
					fields.refs = append(fields.refs, i)
					fields.refsTypes = append(fields.refsTypes, f.Type)
				}
			} else {
				if typeName[0:3] == "[]*" {
					modelType := reflect.TypeOf((*Entity)(nil)).Elem()
					t := f.Type.Elem()
					if t.Implements(modelType) {
						fields.refsMany = append(fields.refsMany, i)
						fields.refsManyTypes = append(fields.refsManyTypes, t)
						continue
					}
				}
				fields.jsons = append(fields.jsons, i)
			}
		}
	}
	return fields
}

func extractTags(registry *Registry, entityType reflect.Type, prefix string) (fields map[string]map[string]string) {
	fields = make(map[string]map[string]string)
	for i := 0; i < entityType.NumField(); i++ {
		field := entityType.Field(i)
		for k, v := range extractTag(registry, field) {
			fields[prefix+k] = v
		}
		_, hasIgnore := fields[field.Name]["ignore"]
		if hasIgnore {
			continue
		}
		refOne := ""
		refMany := ""
		hasRef := false
		hasRefMany := false
		if field.Type.Kind().String() == "ptr" {
			refName := field.Type.Elem().String()
			_, hasRef = registry.entities[refName]
			if hasRef {
				refOne = refName
			}
		} else if field.Type.String()[0:3] == "[]*" {
			refName := field.Type.String()[3:]
			_, hasRefMany = registry.entities[refName]
			if hasRefMany {
				refMany = refName
			}
		}

		query, hasQuery := field.Tag.Lookup("query")
		queryOne, hasQueryOne := field.Tag.Lookup("queryOne")
		if hasQuery {
			if fields[field.Name] == nil {
				fields[field.Name] = make(map[string]string)
			}
			fields[field.Name]["query"] = query
		}
		if hasQueryOne {
			if fields[field.Name] == nil {
				fields[field.Name] = make(map[string]string)
			}
			fields[field.Name]["queryOne"] = queryOne
		}
		if hasRef {
			if fields[field.Name] == nil {
				fields[field.Name] = make(map[string]string)
			}
			fields[field.Name]["ref"] = refOne
		}
		if hasRefMany {
			if fields[field.Name] == nil {
				fields[field.Name] = make(map[string]string)
			}
			fields[field.Name]["refs"] = refMany
		}
	}
	return
}

func extractTag(registry *Registry, field reflect.StructField) map[string]map[string]string {
	tag, ok := field.Tag.Lookup("orm")
	if ok {
		args := strings.Split(tag, ";")
		length := len(args)
		var attributes = make(map[string]string, length)
		for j := 0; j < length; j++ {
			arg := strings.Split(args[j], "=")
			if len(arg) == 1 {
				attributes[arg[0]] = "true"
			} else {
				attributes[arg[0]] = arg[1]
			}
		}
		return map[string]map[string]string{field.Name: attributes}
	} else if field.Type.Kind().String() == "struct" {
		t := field.Type.String()
		if t != "orm.ORM" && t != "time.Time" {
			return extractTags(registry, field.Type, field.Name)
		}
	}
	return make(map[string]map[string]string)
}

func (tableSchema *tableSchema) getCacheKey(id uint64) string {
	return tableSchema.cachePrefix + ":" + strconv.FormatUint(id, 10)
}

func (fields *tableFields) getColumnNames() []string {
	columns := make([]string, 0)
	ids := fields.uintegers
	ids = append(ids, fields.uintegersNullable...)
	ids = append(ids, fields.integers...)
	ids = append(ids, fields.integersNullable...)
	ids = append(ids, fields.strings...)
	ids = append(ids, fields.sliceStrings...)
	ids = append(ids, fields.bytes...)
	if fields.fakeDelete > 0 {
		ids = append(ids, fields.fakeDelete)
	}
	ids = append(ids, fields.booleans...)
	ids = append(ids, fields.booleansNullable...)
	ids = append(ids, fields.floats...)
	ids = append(ids, fields.floatsNullable...)
	ids = append(ids, fields.timesNullable...)
	ids = append(ids, fields.times...)
	ids = append(ids, fields.jsons...)
	ids = append(ids, fields.refs...)
	ids = append(ids, fields.refsMany...)
	for _, i := range ids {
		name := fields.prefix + fields.fields[i].Name
		columns = append(columns, name)
	}
	for _, subFields := range fields.structs {
		columns = append(columns, subFields.getColumnNames()...)
	}
	return columns
}

var defaultRedisSearchMapper = func(val interface{}) interface{} {
	return val
}

var defaultRedisSearchMapperNullableString = func(val interface{}) interface{} {
	if val == nil {
		return ""
	}
	return val
}

var defaultRedisSearchMapperNullableInt = func(val interface{}) interface{} {
	if val == nil {
		return -math.MaxInt64
	}
	return val
}
