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
	GetColumns() map[string]string
	GetUsage(registry *validatedRegistry) (map[reflect.Type][]string, error)
	GetSchemaChanges(engine *Engine) (has bool, alters []Alter, err error)
}

type tableSchema struct {
	tableName        string
	mysqlPoolName    string
	t                reflect.Type
	tags             map[string]map[string]string
	cachedIndexes    map[string]*cachedQueryDefinition
	cachedIndexesOne map[string]*cachedQueryDefinition
	cachedIndexesAll map[string]*cachedQueryDefinition
	columnNames      []string
	columnPathMap    map[string]string
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

func (tableSchema *tableSchema) GetColumns() map[string]string {
	return tableSchema.columnPathMap
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
	tags, columnNames, columnPathMap := extractTags(registry, entityType, "")
	oneRefs := make([]string, 0)
	columnsStamp := fmt.Sprintf("%d", fnv1a.HashString32(fmt.Sprintf("%v", columnNames)))
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

	tableSchema := &tableSchema{tableName: table,
		mysqlPoolName:    mysql,
		t:                entityType,
		tags:             tags,
		columnNames:      columnNames,
		columnPathMap:    columnPathMap,
		columnsStamp:     columnsStamp,
		cachedIndexes:    cachedQueries,
		cachedIndexesOne: cachedQueriesOne,
		cachedIndexesAll: cachedQueriesAll,
		localCacheName:   localCache,
		redisCacheName:   redisCache,
		refOne:           oneRefs,
		cachePrefix:      cachePrefix,
		hasFakeDelete:    hasFakeDelete,
		hasLog:           logPoolName != "",
		logPoolName:      logPoolName,
		logTableName:     fmt.Sprintf("_log_%s_%s", mysql, table)}
	return tableSchema, nil
}

func extractTags(registry *Registry, entityType reflect.Type, prefix string) (fields map[string]map[string]string,
	columnNames []string, columnPathMap map[string]string) {
	fields = make(map[string]map[string]string)
	columnNames = make([]string, 0)
	columnPathMap = make(map[string]string)
	for i := 0; i < entityType.NumField(); i++ {
		field := entityType.Field(i)

		subTags, subFields, subMap := extractTag(registry, field)
		for k, v := range subTags {
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
		if subFields != nil {
			if !hasQuery && !hasQueryOne {
				columnNames = append(columnNames, subFields...)
			}
			for k, v := range subMap {
				columnPathMap[k] = v
			}
		} else if i != 0 || prefix != "" {
			if !hasQuery && !hasQueryOne {
				columnNames = append(columnNames, prefix+field.Name)
				path := strings.TrimLeft(prefix+"."+field.Name, ".")
				if hasRef {
					path += ".ID"
				}
				columnPathMap[path] = prefix + field.Name
			}
		}

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

func extractTag(registry *Registry, field reflect.StructField) (map[string]map[string]string, []string, map[string]string) {
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
		return map[string]map[string]string{field.Name: attributes}, nil, nil
	} else if field.Type.Kind().String() == "struct" {
		t := field.Type.String()
		if t != "orm.ORM" && t != "time.Time" {
			return extractTags(registry, field.Type, field.Name)
		}
	}
	return make(map[string]map[string]string), nil, nil
}

func (tableSchema *tableSchema) getCacheKey(id uint64) string {
	return fmt.Sprintf("%s%s:%d", tableSchema.cachePrefix, tableSchema.columnsStamp, id)
}

func (tableSchema *tableSchema) getCacheKeySearch(indexName string, parameters ...interface{}) string {
	hash := fnv1a.HashString32(fmt.Sprintf("%v", parameters))
	return fmt.Sprintf("%s_%s_%d", tableSchema.cachePrefix, indexName, hash)
}
