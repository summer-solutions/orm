package orm

import (
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/segmentio/fasthash/fnv1a"
)

type CachedQuery struct{}

type cachedQueryDefinition struct {
	Max    int
	Query  string
	Fields []string
}

type TableSchema interface {
	GetTableName() string
	GetType() reflect.Type
	DropTable(engine *Engine) error
	TruncateTable(engine *Engine) error
	UpdateSchema(engine *Engine) error
	UpdateSchemaAndTruncateTable(engine *Engine) error
	GetMysql(engine *Engine) *DB
	GetLocalCache(engine *Engine) (cache *LocalCache, has bool)
	GetRedisCache(engine *Engine) (cache *RedisCache, has bool)
	GetReferences() []string
	GetColumns() []string
	GetUsage(registry *validatedRegistry) (map[reflect.Type][]string, error)
	GetSchemaChanges(engine *Engine) (has bool, alters []Alter, err error)
}

type tableSchema struct {
	tableName        string
	mysqlPoolName    string
	t                reflect.Type
	fields           *tableFields
	fieldsQuery      string
	tags             map[string]map[string]string
	cachedIndexes    map[string]*cachedQueryDefinition
	cachedIndexesOne map[string]*cachedQueryDefinition
	cachedIndexesAll map[string]*cachedQueryDefinition
	columnNames      []string
	uniqueIndices    map[string][]string
	refOne           []string
	columnsStamp     string
	localCacheName   string
	redisCacheName   string
	cachePrefix      string
	hasFakeDelete    bool
	hasLog           bool
	logPoolName      string
	logTableName     string
}

type tableFields struct {
	t             reflect.Type
	fields        map[int]reflect.StructField
	prefix        string
	uintegers     []int
	integers      []int
	strings       []int
	sliceStrings  []int
	bytes         []int
	fakeDelete    int
	booleans      []int
	floats        []int
	timesNullable []int
	times         []int
	jsons         []int
	structs       map[int]*tableFields
	refs          []int
	refsTypes     []reflect.Type
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

func (tableSchema *tableSchema) DropTable(engine *Engine) error {
	pool := tableSchema.GetMysql(engine)
	_, err := pool.Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`;", pool.GetDatabaseName(), tableSchema.tableName))
	return err
}

func (tableSchema *tableSchema) TruncateTable(engine *Engine) error {
	pool := tableSchema.GetMysql(engine)
	_, err := pool.Exec("SET FOREIGN_KEY_CHECKS = 0")
	if err != nil {
		return err
	}
	_, err = pool.Exec(fmt.Sprintf("TRUNCATE TABLE `%s`.`%s`;",
		pool.GetDatabaseName(), tableSchema.tableName))
	if err != nil {
		return err
	}
	_, err = pool.Exec("SET FOREIGN_KEY_CHECKS = 1")
	if err != nil {
		return err
	}
	return nil
}

func (tableSchema *tableSchema) UpdateSchema(engine *Engine) error {
	pool := tableSchema.GetMysql(engine)
	has, alters, err := tableSchema.GetSchemaChanges(engine)
	if err != nil {
		return err
	}
	if has {
		for _, alter := range alters {
			_, err := pool.Exec(alter.SQL)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (tableSchema *tableSchema) UpdateSchemaAndTruncateTable(engine *Engine) error {
	err := tableSchema.UpdateSchema(engine)
	if err != nil {
		return err
	}
	pool := tableSchema.GetMysql(engine)
	_, err = pool.Exec(fmt.Sprintf("TRUNCATE TABLE `%s`.`%s`;", pool.GetDatabaseName(), tableSchema.tableName))
	return err
}

func (tableSchema *tableSchema) GetMysql(engine *Engine) *DB {
	return engine.GetMysql(tableSchema.mysqlPoolName)
}

func (tableSchema *tableSchema) GetLocalCache(engine *Engine) (cache *LocalCache, has bool) {
	if tableSchema.localCacheName == "" {
		return nil, false
	}
	return engine.GetLocalCache(tableSchema.localCacheName), true
}

func (tableSchema *tableSchema) GetRedisCache(engine *Engine) (cache *RedisCache, has bool) {
	if tableSchema.redisCacheName == "" {
		return nil, false
	}
	return engine.GetRedis(tableSchema.redisCacheName), true
}

func (tableSchema *tableSchema) GetReferences() []string {
	return tableSchema.refOne
}

func (tableSchema *tableSchema) GetColumns() []string {
	return tableSchema.columnNames
}

func (tableSchema *tableSchema) GetUsage(registry *validatedRegistry) (map[reflect.Type][]string, error) {
	results := make(map[reflect.Type][]string)
	if registry.entities != nil {
		for _, t := range registry.entities {
			schema := getTableSchema(registry, t)
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
	return results, nil
}

func (tableSchema *tableSchema) GetSchemaChanges(engine *Engine) (has bool, alters []Alter, err error) {
	return getSchemaChanges(engine, tableSchema)
}

func initTableSchema(registry *Registry, entityType reflect.Type) (*tableSchema, error) {
	tags := extractTags(registry, entityType, "")
	oneRefs := make([]string, 0)
	mysql, has := tags["ORM"]["mysql"]
	if !has {
		mysql = "default"
	}
	_, has = registry.sqlClients[mysql]
	if !has {
		return nil, fmt.Errorf("unknown mysql pool '%s'", mysql)
	}
	table, has := tags["ORM"]["table"]
	if !has {
		table = entityType.Name()
	}
	localCache := ""
	redisCache := ""
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
			return nil, fmt.Errorf("unknown local cache pool '%s'", localCache)
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
			return nil, fmt.Errorf("unknown redis pool '%s'", redisCache)
		}
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
		fields := make([]string, 0)
		if has {
			re := regexp.MustCompile(":([A-Za-z0-9])+")
			variables := re.FindAllString(query, -1)
			for _, variable := range variables {
				fieldName := variable[1:]
				if fieldName != "ID" {
					fields = append(fields, fieldName)
				}
				query = strings.Replace(query, variable, fmt.Sprintf("`%s`", fieldName), 1)
			}
			if hasFakeDelete && len(variables) > 0 {
				fields = append(fields, "FakeDelete")
			}
			if query == "" {
				query = "1 ORDER BY `ID`"
			}
			if !isOne {
				max := 50000
				maxAttribute, has := values["max"]
				if has {
					maxFromUser, err := strconv.Atoi(maxAttribute)
					if err != nil {
						return nil, err
					}
					max = maxFromUser
				}
				def := &cachedQueryDefinition{max, query, fields}
				cachedQueries[key] = def
				cachedQueriesAll[key] = def
			} else {
				def := &cachedQueryDefinition{1, query, fields}
				cachedQueriesOne[key] = def
				cachedQueriesAll[key] = def
			}
		}
		_, has = values["ref"]
		if has {
			oneRefs = append(oneRefs, key)
		}
	}
	logPoolName := tags["ORM"]["log"]
	if logPoolName == "true" {
		logPoolName = mysql
	}
	uniqueIndices := make(map[string][]string)
	for k, v := range tags {
		keys, has := v["unique"]
		if has {
			values := strings.Split(keys, ",")
			for _, indexName := range values {
				parts := strings.Split(indexName, ":")
				if uniqueIndices[parts[0]] == nil {
					uniqueIndices[parts[0]] = make([]string, 0)
				}
				uniqueIndices[parts[0]] = append(uniqueIndices[parts[0]], k)
			}
		}
	}
	fields := buildTableFields(entityType, 1, "", tags)
	columns := fields.getColumnNames()
	fieldsQuery := ""
	for _, column := range columns {
		fieldsQuery += ",`" + column + "`"
	}
	columnsStamp := fmt.Sprintf("%d", fnv1a.HashString32(fieldsQuery))

	tableSchema := &tableSchema{tableName: table,
		mysqlPoolName:    mysql,
		t:                entityType,
		fields:           fields,
		fieldsQuery:      fieldsQuery[1:],
		tags:             tags,
		columnNames:      columns,
		columnsStamp:     columnsStamp,
		cachedIndexes:    cachedQueries,
		cachedIndexesOne: cachedQueriesOne,
		cachedIndexesAll: cachedQueriesAll,
		localCacheName:   localCache,
		redisCacheName:   redisCache,
		refOne:           oneRefs,
		cachePrefix:      cachePrefix,
		uniqueIndices:    uniqueIndices,
		hasFakeDelete:    hasFakeDelete,
		hasLog:           logPoolName != "",
		logPoolName:      logPoolName,
		logTableName:     fmt.Sprintf("_log_%s_%s", mysql, table)}
	return tableSchema, nil
}

func buildTableFields(t reflect.Type, start int, prefix string, schemaTags map[string]map[string]string) *tableFields {
	fields := &tableFields{t: t, prefix: prefix, uintegers: make([]int, 0), integers: make([]int, 0), strings: make([]int, 0),
		fields: make(map[int]reflect.StructField), sliceStrings: make([]int, 0),
		bytes: make([]int, 0), booleans: make([]int, 0), floats: make([]int, 0), timesNullable: make([]int, 0), times: make([]int, 0),
		jsons: make([]int, 0), structs: make(map[int]*tableFields), refs: make([]int, 0), refsTypes: make([]reflect.Type, 0)}
	for i := start; i < t.NumField(); i++ {
		f := t.Field(i)
		fields.fields[i] = f
		tags := schemaTags[f.Name]
		typeName := f.Type.String()
		_, has := tags["ignore"]
		if has {
			continue
		}
		switch typeName {
		case "uint",
			"uint8",
			"uint16",
			"uint32",
			"uint64":
			fields.uintegers = append(fields.uintegers, i)
		case "int",
			"int8",
			"int16",
			"int32",
			"int64":
			fields.integers = append(fields.integers, i)
		case "string":
			fields.strings = append(fields.strings, i)
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
		case "float32",
			"float64":
			fields.floats = append(fields.floats, i)
		case "*time.Time":
			fields.timesNullable = append(fields.timesNullable, i)
		case "time.Time":
			fields.times = append(fields.times, i)
		case "interface {}":
			fields.jsons = append(fields.jsons, i)
		default:
			k := f.Type.Kind().String()
			if k == "struct" {
				fields.structs[i] = buildTableFields(f.Type, 0, f.Name, schemaTags)
			} else if k == "ptr" {
				modelType := reflect.TypeOf((*Entity)(nil)).Elem()
				if f.Type.Implements(modelType) {
					fields.refs = append(fields.refs, i)
					fields.refsTypes = append(fields.refsTypes, f.Type)
				}
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
		hasRef := false
		if field.Type.Kind().String() == "ptr" {
			refName := field.Type.Elem().String()
			_, hasRef = registry.entities[refName]
			if hasRef {
				refOne = refName
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
	return tableSchema.cachePrefix + ":" + tableSchema.columnsStamp + ":" + strconv.FormatUint(id, 10)
}

func (fields *tableFields) getColumnNames() []string {
	columns := make([]string, 0)
	ids := fields.uintegers
	ids = append(ids, fields.integers...)
	ids = append(ids, fields.strings...)
	ids = append(ids, fields.sliceStrings...)
	ids = append(ids, fields.bytes...)
	if fields.fakeDelete > 0 {
		ids = append(ids, fields.fakeDelete)
	}
	ids = append(ids, fields.booleans...)
	ids = append(ids, fields.floats...)
	ids = append(ids, fields.timesNullable...)
	ids = append(ids, fields.times...)
	ids = append(ids, fields.jsons...)
	ids = append(ids, fields.refs...)
	for _, i := range ids {
		name := fields.prefix + fields.fields[i].Name
		columns = append(columns, name)
	}
	for _, subFields := range fields.structs {
		columns = append(columns, subFields.getColumnNames()...)
	}
	return columns
}
