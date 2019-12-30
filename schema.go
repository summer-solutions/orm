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
	e      interface{}
}

type CachedQuery struct{}

func (orm *ORM) MarkToDelete() {
	orm.dBData["_delete"] = true
}

func (orm *ORM) IsDirty() bool {
	if orm.e == nil {
		return false
	}
	if orm.dBData["_delete"] == true {
		return true
	}
	v := reflect.ValueOf(orm.e)
	value := reflect.Indirect(v)
	t := value.Type()
	bind, err := createBind(getTableSchema(t), t, value, orm.dBData, "")
	if err != nil {
		panic(err.Error())
	}
	return value.Field(1).Uint() == 0 || len(bind) > 0
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
	TableName      string
	MysqlPoolName  string
	t              reflect.Type
	tags           map[string]map[string]string
	cachedIndexes  map[string]cachedQueryDefinition
	columnNames    []string
	refOne         []string
	refMany        []string
	columnsStamp   string
	localCacheName string
	redisCacheName string
	cachePrefix    string
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

var tableSchemas = make(map[reflect.Type]*TableSchema)

func GetTableSchema(entity interface{}) *TableSchema {
	return getTableSchema(reflect.TypeOf(entity))
}

func getTableSchema(entityType reflect.Type) *TableSchema {
	tableSchema, has := tableSchemas[entityType]
	if has {
		return tableSchema
	}
	tags, columnNames := tableSchema.extractTags(entityType, "")
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
	for key, values := range tags {
		query, has := values["query"]
		fields := make([]string, 0)
		if has {
			re := regexp.MustCompile(":([A-Za-z0-9])+")
			variables := re.FindAllString(query, -1)
			for _, variable := range variables {
				fieldName := variable[1:]
				if fieldName != "Id" {
					fields = append(fields, fieldName)
				}
				query = strings.Replace(query, variable, fmt.Sprintf("`%s`", fieldName), 1)
			}
			if query == "" {
				query = "1"
			}
			max := 1000
			maxAttribute, has := values["max"]
			if has {
				maxFromUser, err := strconv.Atoi(maxAttribute)
				if err != nil {
					panic(fmt.Errorf("invalid max value for cache index %s", maxAttribute))
				}
				max = maxFromUser
			}
			cachedQueries[key] = cachedQueryDefinition{max, query, fields}
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
		MysqlPoolName:  mysql,
		t:              entityType,
		tags:           tags,
		columnNames:    columnNames,
		columnsStamp:   columnsStamp,
		cachedIndexes:  cachedQueries,
		localCacheName: localCache,
		redisCacheName: redisCache,
		refOne:         oneRefs,
		refMany:        manyRefs,
		cachePrefix:    cachePrefix}
	tableSchemas[entityType] = tableSchema
	return tableSchema
}

func (tableSchema TableSchema) GetSchemaChanges() (has bool, alter Alter, err error) {
	indexes := make(map[string]*index)
	columns := tableSchema.checkStruct(tableSchema.t, indexes, "")

	createTableSql := fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n", GetMysql(tableSchema.MysqlPoolName).databaseName, tableSchema.TableName)
	columns[0][1] += " AUTO_INCREMENT"
	for _, value := range columns {
		createTableSql += fmt.Sprintf("  %s,\n", value[1])
	}
	createTableSql += fmt.Sprint("  PRIMARY KEY (`Id`)\n")
	createTableSql += fmt.Sprint(") ENGINE=InnoDB DEFAULT CHARSET=utf8;")

	var skip string
	err = tableSchema.GetMysql().QueryRow(fmt.Sprintf("SHOW TABLES LIKE '%s'", tableSchema.TableName)).Scan(&skip)
	hasTable := true
	if err != nil {
		hasTable = false
	}

	if !hasTable {
		alter = Alter{Sql: createTableSql, Safe: true, Pool: tableSchema.MysqlPoolName}
		has = true
		err = nil
		return
	}

	var tableDBColumns = make([][2]string, 0)
	var createTableDB string
	err = tableSchema.GetMysql().QueryRow(fmt.Sprintf("SHOW CREATE TABLE `%s`", tableSchema.TableName)).Scan(&skip, &createTableDB)
	if err != nil {
		return false, Alter{}, err
	}
	lines := strings.Split(createTableDB, "\n")
	for x := 1; x < len(lines); x++ {
		if lines[x][2] != 96 {
			continue
		}
		var line = strings.TrimRight(lines[x], ",")
		line = strings.TrimLeft(line, " ")
		var columnName = strings.Split(line, "`")[1]
		tableDBColumns = append(tableDBColumns, [2]string{columnName, line})
	}

	var rows []indexDB
	results, err := tableSchema.GetMysql().Query(fmt.Sprintf("SHOW INDEXES FROM `%s`", tableSchema.TableName))
	if err != nil {
		return false, Alter{}, err
	}
	for results.Next() {
		var row indexDB
		err = results.Scan(&row.Skip, &row.NonUnique, &row.KeyName, &row.Seq, &row.Column, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip, &row.Skip)
		if err != nil {
			return false, Alter{}, err
		}
		rows = append(rows, row)
	}
	var indexesDB = make(map[string]*index)
	for _, value := range rows {
		current, has := indexesDB[value.KeyName]
		if !has {
			current = &index{Unique: value.NonUnique == 0, Columns: map[int]string{value.Seq: value.Column}}
			indexesDB[value.KeyName] = current
		} else {
			current.Columns[value.Seq] = value.Column
		}
	}

	var newColumns []string
	var changedColumns [][2]string

	hasAlters := false
	for key, value := range columns {
		var tableColumn string
		if key < len(tableDBColumns) {
			tableColumn = tableDBColumns[key][1]
		}
		if tableColumn == value[1] {
			continue
		}
		hasName := -1
		hasDefinition := -1
		for z, v := range tableDBColumns {
			if v[1] == value[1] {
				hasDefinition = z
			}
			if v[0] == value[0] {
				hasName = z
			}
		}
		if hasName == -1 {
			alter := fmt.Sprintf("ADD COLUMN %s", value[1])
			if key > 0 {
				alter += fmt.Sprintf(" AFTER `%s`", columns[key-1][0])
			}
			newColumns = append(newColumns, alter)
			hasAlters = true
		} else {
			if hasDefinition == -1 {
				alter := fmt.Sprintf("CHANGE COLUMN `%s` %s", value[0], value[1])
				if key > 0 {
					alter += fmt.Sprintf(" AFTER `%s`", columns[key-1][0])
				}
				changedColumns = append(changedColumns, [2]string{alter, fmt.Sprintf("CHANGED FROM %s", tableDBColumns[hasName][1])})
				hasAlters = true
			} else {
				alter := fmt.Sprintf("CHANGE COLUMN `%s` %s", value[0], value[1])
				if key > 0 {
					alter += fmt.Sprintf(" AFTER `%s`", columns[key-1][0])
				}
				changedColumns = append(changedColumns, [2]string{alter, "CHANGED ORDER"})
				hasAlters = true
			}
		}
	}
	droppedColumns := make([]string, 0)
OUTER:
	for _, value := range tableDBColumns {
		for _, v := range columns {
			if v[0] == value[0] {
				continue OUTER
			}
		}
		droppedColumns = append(droppedColumns, fmt.Sprintf("DROP COLUMN `%s`", value[0]))
		hasAlters = true
	}

	var droppedIndexes []string
	var newIndexes []string
	for keyName, indexEntity := range indexes {
		indexDB, has := indexesDB[keyName]
		if !has {
			newIndexes = append(newIndexes, tableSchema.buildCreateIndexSql(keyName, indexEntity))
			hasAlters = true
		} else {
			addIndexSqlEntity := tableSchema.buildCreateIndexSql(keyName, indexEntity)
			addIndexSqlDB := tableSchema.buildCreateIndexSql(keyName, indexDB)
			if addIndexSqlEntity != addIndexSqlDB {
				droppedIndexes = append(droppedIndexes, fmt.Sprintf("DROP INDEX `%s`", keyName))
				newIndexes = append(newIndexes, addIndexSqlEntity)
				hasAlters = true
			}
		}
	}
	for keyName := range indexesDB {
		_, has := indexes[keyName]
		if !has && keyName != "PRIMARY" {
			droppedIndexes = append(droppedIndexes, fmt.Sprintf("DROP INDEX `%s`", keyName))
			hasAlters = true
		}
	}

	if !hasAlters {
		return
	}
	alterSql := fmt.Sprintf("ALTER TABLE `%s`.`%s`\n", GetMysql(tableSchema.MysqlPoolName).databaseName, tableSchema.TableName)
	alters := make([]string, 0)
	comments := make([]string, 0)
	for _, value := range droppedColumns {
		alters = append(alters, fmt.Sprintf("    %s", value))
		comments = append(comments, "")
	}
	for _, value := range newColumns {
		alters = append(alters, fmt.Sprintf("    %s", value))
		comments = append(comments, "")
	}
	for _, value := range changedColumns {
		alters = append(alters, fmt.Sprintf("    %s", value[0]))
		comments = append(comments, value[1])
	}
	for _, value := range droppedIndexes {
		alters = append(alters, fmt.Sprintf("    %s", value))
		comments = append(comments, "")
	}
	for _, value := range newIndexes {
		alters = append(alters, fmt.Sprintf("    %s", value))
		comments = append(comments, "")
	}
	for x := 0; x < len(alters)-1; x++ {
		alterSql += alters[x] + ","
		if comments[x] != "" {
			alterSql += fmt.Sprintf("/*%s*/", comments[x])
		}
		alterSql += "\n"
	}
	lastIndex := len(alters) - 1
	if lastIndex >= 0 {
		alterSql += alters[lastIndex] + ";"
		if comments[lastIndex] != "" {
			alterSql += fmt.Sprintf("/*%s*/", comments[lastIndex])
		}
	}

	if tableSchema.isTableEmpty() || (len(droppedColumns) == 0 && len(changedColumns) == 0) {
		alter = Alter{Sql: alterSql, Safe: true, Pool: tableSchema.MysqlPoolName}
	} else {
		alter = Alter{Sql: alterSql, Safe: false, Pool: tableSchema.MysqlPoolName}
	}
	has = true
	return has, alter, nil
}

func (tableSchema TableSchema) DropTable() error {
	_, err := tableSchema.GetMysql().Exec(fmt.Sprintf("DROP TABLE IF EXISTS `%s`", tableSchema.TableName))
	return err
}

func (tableSchema TableSchema) UpdateSchema() error {
	has, alter, err := tableSchema.GetSchemaChanges()
	if err != nil {
		return err
	}
	if has {
		_, err := tableSchema.GetMysql().Exec(alter.Sql)
		if err != nil {
			return err
		}
	}
	return nil
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

func (tableSchema TableSchema) isTableEmpty() bool {
	var lastId uint64
	err := tableSchema.GetMysql().QueryRow(fmt.Sprintf("SELECT `Id` FROM `%s` LIMIT 1", tableSchema.TableName)).Scan(&lastId)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return true
		}
		panic(err.Error())
	}
	return false
}

func (tableSchema TableSchema) checkStruct(t reflect.Type, indexes map[string]*index, prefix string) (columns [][2]string) {
	columns = make([][2]string, 0, t.NumField())
	max := t.NumField() - 1
	for i := 0; i <= max; i++ {
		if i == 0 && prefix == "" {
			continue
		}
		field := t.Field(i)
		var fieldColumns = tableSchema.checkColumn(&field, indexes, prefix)
		if fieldColumns != nil {
			columns = append(columns, fieldColumns...)
		}
	}
	return
}

func (tableSchema TableSchema) checkColumn(field *reflect.StructField, indexes map[string]*index, prefix string) [][2]string {
	var definition string
	var addNotNullIfNotSet bool
	addDefaultNullIfNullable := true
	var typeAsString = field.Type.String()
	columnName := prefix + field.Name

	attributes := tableSchema.tags[columnName]

	indexAttribute, has := attributes["index"]
	unique := false
	if !has {
		indexAttribute, has = attributes["unique"]
		unique = true
	}
	if has {
		indexColumns := strings.Split(indexAttribute, ",")
		for _, value := range indexColumns {
			indexColumn := strings.Split(value, ":")
			location := 1
			if len(indexColumn) > 1 {
				userLocation, err := strconv.Atoi(indexColumn[1])
				if err != nil {
					panic(err.Error())
				}
				location = userLocation
			}
			current, has := indexes[indexColumn[0]]
			if !has {
				current = &index{Unique: unique, Columns: map[int]string{location: field.Name}}
				indexes[indexColumn[0]] = current
			} else {
				current.Columns[location] = field.Name
			}
		}
	}

	switch typeAsString {
	case "uint":
		definition, addNotNullIfNotSet = tableSchema.handleInt("int(10) unsigned")
	case "uint8":
		definition, addNotNullIfNotSet = tableSchema.handleInt("tinyint(3) unsigned")
	case "uint16":
		yearAttribute, _ := attributes["year"]
		if yearAttribute == "true" {
			return [][2]string{{columnName, fmt.Sprintf("`%s` year(4) NOT NULL DEFAULT '0000'", columnName)}}
		} else {
			definition, addNotNullIfNotSet = tableSchema.handleInt("smallint(5) unsigned")
		}
	case "uint32":
		mediumIntAttribute, _ := attributes["mediumint"]
		if mediumIntAttribute == "true" {
			definition, addNotNullIfNotSet = tableSchema.handleInt("mediumint(8) unsigned")
		} else {
			definition, addNotNullIfNotSet = tableSchema.handleInt("int(10) unsigned")
		}
	case "uint64":
		definition, addNotNullIfNotSet = tableSchema.handleInt("bigint(20) unsigned")
	case "int8":
		definition, addNotNullIfNotSet = tableSchema.handleInt("tinyint(4)")
	case "int16":
		definition, addNotNullIfNotSet = tableSchema.handleInt("smallint(6)")
	case "int32":
		mediumIntAttribute, _ := attributes["mediumint"]
		if mediumIntAttribute == "true" {
			definition, addNotNullIfNotSet = tableSchema.handleInt("mediumint(9)")
		} else {
			definition, addNotNullIfNotSet = tableSchema.handleInt("int(11)")
		}
	case "int64":
		definition, addNotNullIfNotSet = tableSchema.handleInt("bigint(20)")
	case "rune":
		definition, addNotNullIfNotSet = tableSchema.handleInt("int(11)")
	case "int":
		definition, addNotNullIfNotSet = tableSchema.handleInt("int(11)")
	case "bool":
		definition, addNotNullIfNotSet = tableSchema.handleInt("tinyint(1)")
	case "string", "[]string":
		definition, addNotNullIfNotSet, addDefaultNullIfNullable = tableSchema.handleString(attributes, false)
	case "interface {}", "[]uint64":
		definition, addNotNullIfNotSet, addDefaultNullIfNullable = tableSchema.handleString(attributes, true)
	case "float32":
		definition, addNotNullIfNotSet = tableSchema.handleFloat("float", attributes)
	case "float64":
		definition, addNotNullIfNotSet = tableSchema.handleFloat("double", attributes)
	case "time.Time":
		definition, addNotNullIfNotSet, addDefaultNullIfNullable = tableSchema.handleTime(attributes)
	case "*orm.ReferenceOne":
		definition = tableSchema.handleReferenceOne(attributes)
		addNotNullIfNotSet = true
		addDefaultNullIfNullable = true
	case "*orm.ReferenceMany":
		definition = tableSchema.handleReferenceMany(attributes)
		addNotNullIfNotSet = false
		addDefaultNullIfNullable = false
	case "*orm.CachedQuery":
		return nil
	default:
		kind := field.Type.Kind().String()
		if kind == "struct" {
			structFields := tableSchema.checkStruct(field.Type, indexes, field.Name)
			return structFields
		}
		panic(fmt.Errorf("unsoported field type: %s %s", field.Name, field.Type.String()))
	}
	isNotNull := false
	if addNotNullIfNotSet {
		definition += " NOT NULL"
		isNotNull = true
	}
	if !isNotNull && addDefaultNullIfNullable {
		definition += " DEFAULT NULL"
	}
	return [][2]string{{columnName, fmt.Sprintf("`%s` %s", columnName, definition)}}
}

func (tableSchema TableSchema) handleInt(definition string) (string, bool) {
	return definition, true
}

func (tableSchema TableSchema) handleFloat(floatDefinition string, attributes map[string]string) (string, bool) {
	decimal, hasDecimal := attributes["decimal"]
	var definition string
	if hasDecimal {
		decimalArgs := strings.Split(decimal, ",")
		definition = fmt.Sprintf("decimal(%s,%s)", decimalArgs[0], decimalArgs[1])
	} else {
		definition = floatDefinition
	}
	unsigned, hasUnsigned := attributes["unsigned"]
	if !hasUnsigned || unsigned == "true" {
		definition += " unsigned"
	}
	return definition, true
}

func (tableSchema TableSchema) handleString(attributes map[string]string, forceMax bool) (string, bool, bool) {
	var definition string
	enum, hasEnum := attributes["enum"]
	if hasEnum {
		return handleSetEnum("enum", enum)
	}
	set, haSet := attributes["set"]
	if haSet {
		return handleSetEnum("set", set)
	}
	var addDefaultNullIfNullable = true
	length, hasLength := attributes["length"]
	if hasLength == false {
		length = "255"
	}
	if forceMax || length == "max" {
		definition = "mediumtext"
		addDefaultNullIfNullable = false
	} else {
		i, err := strconv.Atoi(length)
		if err != nil {
			panic(fmt.Errorf("wrong lenght: %s", length))
		}
		if i > 65535 {
			panic(fmt.Errorf("lenght to heigh: %s", length))
		}
		definition = fmt.Sprintf("varchar(%s)", strconv.Itoa(i))
	}
	return definition, false, addDefaultNullIfNullable
}

func handleSetEnum(fieldType string, attribute string) (string, bool, bool) {
	values := strings.Split(attribute, ",")
	var definition = fieldType + "("
	for key, value := range values {
		if key > 0 {
			definition += ","
		}
		definition += fmt.Sprintf("'%s'", value)
	}
	definition += ")"
	return definition, false, true
}

func (tableSchema TableSchema) handleTime(attributes map[string]string) (string, bool, bool) {
	time, _ := attributes["time"]
	if time == "true" {
		return "datetime", true, true
	}
	return "date", true, true
}

func (tableSchema TableSchema) handleReferenceOne(attributes map[string]string) string {
	reference, has := attributes["ref"]
	if !has {
		panic(fmt.Errorf("missing ref tag"))
	}
	typeAsString := getEntityType(reference).Field(1).Type.String()
	switch typeAsString {
	case "uint":
		return "int(10) unsigned"
	case "uint8":
		return "tinyint(3) unsigned"
	case "uint16":
		return "smallint(5) unsigned"
	case "uint32":
		mediumIntAttribute, _ := attributes["mediumint"]
		if mediumIntAttribute == "true" {
			return "mediumint(8) unsigned"
		} else {
			return "int(10) unsigned"
		}
	case "uint64":
		return "bigint(20) unsigned"
	}
	return "int(10) unsigned"
}

func (tableSchema TableSchema) handleReferenceMany(attributes map[string]string) string {
	_, has := attributes["ref"]
	if !has {
		panic(fmt.Errorf("missing ref tag"))
	}
	return "varchar(5000)"
}

func (tableSchema TableSchema) buildCreateIndexSql(keyName string, definition *index) string {
	var indexColumns []string
	for i := 1; i <= 100; i++ {
		value, has := definition.Columns[i]
		if has {
			indexColumns = append(indexColumns, fmt.Sprintf("`%s`", value))
		} else {
			break
		}
	}
	indexType := "INDEX"
	if definition.Unique {
		indexType = "UNIQUE " + indexType
	}
	return fmt.Sprintf("ADD %s `%s` (%s)", indexType, keyName, strings.Join(indexColumns, ","))
}

func (tableSchema *TableSchema) extractTags(entityType reflect.Type, prefix string) (fields map[string]map[string]string, columnNames []string) {
	fields = make(map[string]map[string]string)
	columnNames = make([]string, 0)
	for i := 0; i < entityType.NumField(); i++ {
		field := entityType.Field(i)

		subTags, subFields := tableSchema.extractTag(field)
		for k, v := range subTags {
			fields[prefix+k] = v
		}
		if subFields != nil {
			columnNames = append(columnNames, subFields...)
		} else if i != 0 || prefix != "" {
			columnNames = append(columnNames, prefix+field.Name)
		}

		query, has := field.Tag.Lookup("query")
		if has {
			if fields[field.Name] == nil {
				fields[field.Name] = make(map[string]string)
			}
			fields[field.Name]["query"] = query
		}
		_, has = fields[field.Name]["ref"]
		if has {
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

func (tableSchema *TableSchema) extractTag(field reflect.StructField) (map[string]map[string]string, []string) {
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
		return map[string]map[string]string{field.Name: attributes}, nil
	} else if field.Type.Kind().String() == "struct" {
		if field.Type.String() != "time.Time" {
			return tableSchema.extractTags(field.Type, field.Name)
		}
	}
	return make(map[string]map[string]string), nil
}
