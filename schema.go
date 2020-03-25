package orm

import (
	"crypto/md5"
	"database/sql"
	"fmt"
	"reflect"
	"regexp"
	"strconv"
	"strings"
)

type ORM struct {
	dBData map[string]interface{}
	elem   reflect.Value
}

type CachedQuery struct{}

func (orm *ORM) MarkToDelete() {
	orm.dBData["_delete"] = true
}

func (orm *ORM) IsDirty() bool {
	if orm.dBData["_delete"] == true {
		return true
	}
	t := orm.elem.Type()
	bind, err := createBind(getTableSchema(t), t, orm.elem, orm.dBData, "")
	if err != nil {
		panic(err.Error())
	}
	return orm.elem.Field(1).Uint() == 0 || len(bind) > 0
}

func (orm *ORM) isDirty(value reflect.Value) (is bool, bind map[string]interface{}, err error) {
	t := value.Type()
	ormField := value.Field(0).Interface().(*ORM)
	if ormField.dBData["_delete"] == true {
		return true, nil, nil
	}
	bind, err = createBind(getTableSchema(t), t, value, ormField.dBData, "")
	if err != nil {
		return false, nil, err
	}
	is = value.Field(1).Uint() == 0 || len(bind) > 0
	return is, bind, nil
}

type cachedQueryDefinition struct {
	Max    int
	Query  string
	Fields []string
}

type TableSchema struct {
	TableName        string
	MysqlPoolName    string
	t                reflect.Type
	Tags             map[string]map[string]string
	cachedIndexes    map[string]cachedQueryDefinition
	cachedIndexesOne map[string]cachedQueryDefinition
	columnNames      []string
	columnPathMap    map[string]string
	refOne           []string
	refMany          []string
	columnsStamp     string
	localCacheName   string
	redisCacheName   string
	cachePrefix      string
}

type indexDB struct {
	Skip      sql.NullString
	NonUnique uint8
	KeyName   string
	Seq       int
	Column    string
}

type index struct {
	Unique  bool
	Columns map[int]string
}

type foreignIndex struct {
	Column         string
	Table          string
	ParentDatabase string
	OnDelete       string
}

type foreignKeyDB struct {
	ConstraintName        string
	ColumnName            string
	ReferencedTableName   string
	ReferencedTableSchema string
	OnDelete              string
}

var tableSchemas = make(map[reflect.Type]*TableSchema)

func GetTableSchema(entityOrType interface{}) *TableSchema {
	asType, ok := entityOrType.(reflect.Type)
	if ok {
		return getTableSchema(asType)
	}
	return getTableSchema(reflect.TypeOf(entityOrType))
}

func getTableSchema(entityType reflect.Type) *TableSchema {
	tableSchema, has := tableSchemas[entityType]
	if has {
		return tableSchema
	}
	tags, columnNames, columnPathMap := tableSchema.extractTags(entityType, "")
	oneRefs := make([]string, 0)
	manyRefs := make([]string, 0)
	md5Part := md5.Sum([]byte(fmt.Sprintf("%v", columnNames)))
	columnsStamp := fmt.Sprintf("%x", md5Part[:1])
	mysql, has := tags["Orm"]["mysql"]
	if !has {
		mysql = "default"
	}
	table, has := tags["Orm"]["table"]
	if !has {
		table = entityType.Name()
	}
	localCache := ""
	redisCache := ""
	userValue, has := tags["Orm"]["localCache"]
	if has {
		if userValue == "true" {
			userValue = "default"
		}
		localCache = userValue
	}
	userValue, has = tags["Orm"]["redisCache"]
	if has {
		if userValue == "true" {
			userValue = "default"
		}
		redisCache = userValue
	}
	cachePrefix := ""
	if mysql != "default" {
		cachePrefix = mysql
	}
	cachePrefix += table
	cachedQueries := make(map[string]cachedQueryDefinition)
	cachedQueriesOne := make(map[string]cachedQueryDefinition)
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
				if fieldName != "Id" && fieldName != "ID" {
					fields = append(fields, fieldName)
				}
				query = strings.Replace(query, variable, fmt.Sprintf("`%s`", fieldName), 1)
			}
			if query == "" {
				query = "1 ORDER BY `Id`"
			}
			if !isOne {
				max := 50000
				maxAttribute, has := values["max"]
				if has {
					maxFromUser, err := strconv.Atoi(maxAttribute)
					if err != nil {
						panic(fmt.Errorf("invalid max value for cache index %s", maxAttribute))
					}
					max = maxFromUser
				}
				cachedQueries[key] = cachedQueryDefinition{max, query, fields}
			} else {
				cachedQueriesOne[key] = cachedQueryDefinition{1, query, fields}
			}
		}
		userValue, has = values["refType"]
		if has {
			if userValue == "one" {
				oneRefs = append(oneRefs, key)
			} else {
				manyRefs = append(manyRefs, key)
			}
		}
	}
	tableSchema = &TableSchema{TableName: table,
		MysqlPoolName:    mysql,
		t:                entityType,
		Tags:             tags,
		columnNames:      columnNames,
		columnPathMap:    columnPathMap,
		columnsStamp:     columnsStamp,
		cachedIndexes:    cachedQueries,
		cachedIndexesOne: cachedQueriesOne,
		localCacheName:   localCache,
		redisCacheName:   redisCache,
		refOne:           oneRefs,
		refMany:          manyRefs,
		cachePrefix:      cachePrefix}
	tableSchemas[entityType] = tableSchema
	return tableSchema
}

func (tableSchema TableSchema) GetSchemaChanges() (has bool, alters []Alter, err error) {
	return tableSchema.GetMysql().databaseInterface.GetSchemaChanges(tableSchema)
}

func (tableSchema TableSchema) DropTable() error {
	_, err := tableSchema.GetMysql().Exec(tableSchema.GetMysql().databaseInterface.GetDropTableQuery(tableSchema.GetMysql().databaseName, tableSchema.TableName))
	return err
}

func (tableSchema TableSchema) TruncateTable() error {
	_, err := tableSchema.GetMysql().Exec("SET FOREIGN_KEY_CHECKS = 0")
	if err != nil {
		return err
	}
	_, err = tableSchema.GetMysql().Exec(tableSchema.GetMysql().databaseInterface.GetTruncateTableQuery(tableSchema.GetMysql().databaseName, tableSchema.TableName))
	if err != nil {
		return err
	}
	_, err = tableSchema.GetMysql().Exec("SET FOREIGN_KEY_CHECKS = 1")
	if err != nil {
		return err
	}
	return nil
}

func (tableSchema TableSchema) UpdateSchema() error {
	has, alters, err := tableSchema.GetSchemaChanges()
	if err != nil {
		return err
	}
	if has {
		for _, alter := range alters {
			_, err := tableSchema.GetMysql().Exec(alter.Sql)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (tableSchema TableSchema) UpdateSchemaAndTruncateTable() error {
	err := tableSchema.UpdateSchema()
	if err != nil {
		return err
	}
	_, err = tableSchema.GetMysql().Exec(tableSchema.GetMysql().databaseInterface.GetTruncateTableQuery(tableSchema.GetMysql().databaseName, tableSchema.TableName))
	return err
}

func (tableSchema TableSchema) GetMysql() *DB {
	return GetMysql(tableSchema.MysqlPoolName)
}

func (tableSchema TableSchema) GetLocalCache() *LocalCache {
	if tableSchema.localCacheName == "" {
		return nil
	}
	return GetLocalCache(tableSchema.localCacheName)
}

func (tableSchema TableSchema) GetRedisCacheContainer() *RedisCache {
	if tableSchema.redisCacheName == "" {
		return nil
	}
	return GetRedis(tableSchema.redisCacheName)
}

func (tableSchema TableSchema) getCacheKey(id uint64) string {
	return fmt.Sprintf("%s%s:%d", tableSchema.cachePrefix, tableSchema.columnsStamp, id)
}

func (tableSchema TableSchema) getCacheKeySearch(indexName string, parameters ...interface{}) string {
	md5Part := md5.Sum([]byte(fmt.Sprintf("%v", parameters)))
	return fmt.Sprintf("%s_%s_%x", tableSchema.cachePrefix, indexName, md5Part[:5])
}

func (tableSchema *TableSchema) GetColumns() map[string]string {
	return tableSchema.columnPathMap
}

func (tableSchema *TableSchema) GetUsage() map[reflect.Type][]string {
	results := make(map[reflect.Type][]string)
	for _, t := range entities {
		schema := GetTableSchema(t)
		for _, columnName := range append(schema.refOne, schema.refMany...) {
			ref, has := schema.Tags[columnName]["ref"]
			if has && ref == tableSchema.t.String() {
				if results[t] == nil {
					results[t] = make([]string, 0)
				}
				results[t] = append(results[t], columnName)
			}
		}
	}
	return results
}

func (tableSchema *TableSchema) extractTags(entityType reflect.Type, prefix string) (fields map[string]map[string]string,
	columnNames []string, columnPathMap map[string]string) {
	fields = make(map[string]map[string]string)
	columnNames = make([]string, 0)
	columnPathMap = make(map[string]string)
	for i := 0; i < entityType.NumField(); i++ {
		field := entityType.Field(i)

		subTags, subFields, subMap := tableSchema.extractTag(field)
		for k, v := range subTags {
			fields[prefix+k] = v
		}
		_, hasIgnore := fields[field.Name]["ignore"]
		if hasIgnore {
			continue
		}
		_, hasRef := fields[field.Name]["ref"]
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
					path += ".Id"
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
			refType := "one"
			if field.Type.String() == "*orm.ReferenceMany" {
				refType = "many"
			}
			fields[field.Name]["refType"] = refType
		}
	}
	return
}

func (tableSchema *TableSchema) extractTag(field reflect.StructField) (map[string]map[string]string, []string, map[string]string) {
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
		if field.Type.String() != "time.Time" {
			return tableSchema.extractTags(field.Type, field.Name)
		}
	}
	return make(map[string]map[string]string), nil, nil
}
