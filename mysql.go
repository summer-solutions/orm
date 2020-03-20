package orm

import (
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"math"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

type Mysql struct{}

func (m Mysql) GetDatabaseName(db *sql.DB) (string, error) {
	var dbName string
	err := db.QueryRow("SELECT DATABASE()").Scan(&dbName)
	if err != nil {
		return "", err
	}
	return dbName, nil
}

func (m Mysql) InitDb(db *sql.DB) error {
	var variable string
	var maxConnections float64
	var maxTime float64
	err := db.QueryRow("SHOW VARIABLES LIKE 'max_connections'").Scan(&variable, &maxConnections)
	if err != nil {
		return err
	}
	err = db.QueryRow("SHOW VARIABLES LIKE 'interactive_timeout'").Scan(&variable, &maxTime)
	if err != nil {
		return err
	}
	maxConnectionsOrm := math.Ceil(maxConnections * 0.9)
	maxIdleConnections := math.Ceil(maxConnections * 0.05)
	maxConnectionsTime := math.Ceil(maxTime * 0.7)
	if maxIdleConnections < 10 {
		maxIdleConnections = maxConnectionsOrm
	}

	db.SetMaxOpenConns(int(maxConnectionsOrm))
	db.SetMaxIdleConns(int(maxIdleConnections))
	db.SetConnMaxLifetime(time.Duration(int(maxConnectionsTime)) * time.Second)
	return nil
}

func (m Mysql) IsTableEmpty(db *sql.DB, tableName string) (bool, error) {
	var lastId uint64
	err := db.QueryRow(fmt.Sprintf("SELECT `Id` FROM `%s` LIMIT 1", tableName)).Scan(&lastId)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

func (m Mysql) GetUpdateQuery(tableName string, fields []string) string {
	return fmt.Sprintf("UPDATE %s SET %s WHERE `Id` = ?", tableName, strings.Join(fields, ","))
}

func (m Mysql) GetInsertQuery(tableName string, fields []string, values string) string {
	return fmt.Sprintf("INSERT INTO %s(%s) VALUES %s", tableName, strings.Join(fields, ","), values)
}

func (m Mysql) GetDeleteQuery(tableName string, ids []interface{}) string {
	where := NewWhere("`Id` IN ?", ids)
	return fmt.Sprintf("DELETE FROM `%s` WHERE %s", tableName, where)
}

func (m Mysql) ConvertToDuplicateKeyError(err error) error {
	sqlErr, yes := err.(*mysql.MySQLError)
	if yes && sqlErr.Number == 1062 {
		var abortLabelReg, _ = regexp.Compile(` for key '(.*?)'`)
		labels := abortLabelReg.FindStringSubmatch(sqlErr.Message)
		if len(labels) > 0 {
			return &DuplicatedKeyError{Message: sqlErr.Message, Index: labels[1]}
		}
	}
	return err
}

func (m Mysql) Limit(pager *Pager) string {
	return fmt.Sprintf("LIMIT %d,%d", (pager.CurrentPage-1)*pager.PageSize, pager.PageSize)
}

func (m Mysql) GetAllTables(db *sql.DB) ([]string, error) {
	tables := make([]string, 0)
	results, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	for results.Next() {
		var row string
		err = results.Scan(&row)
		if err != nil {
			return nil, err
		}
		tables = append(tables, row)
	}
	return tables, nil
}

func (m Mysql) GetDropTableQuery(database string, table string) string {
	return fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`;", database, table)
}

func (m Mysql) GetTruncateTableQuery(database string, table string) string {
	return fmt.Sprintf("TRUNCATE TABLE `%s`.`%s`;", database, table)
}

func (m Mysql) GetSchemaChanges(tableSchema TableSchema) (has bool, alter Alter, err error) {
	indexes := make(map[string]*index)
	columns, err := m.checkStruct(tableSchema, tableSchema.t, indexes, "")
	if err != nil {
		return false, Alter{}, err
	}
	var newIndexes []string

	createTableSql := fmt.Sprintf("CREATE TABLE `%s`.`%s` (\n", GetMysql(tableSchema.MysqlPoolName).databaseName, tableSchema.TableName)
	columns[0][1] += " AUTO_INCREMENT"
	for _, value := range columns {
		createTableSql += fmt.Sprintf("  %s,\n", value[1])
	}
	for keyName, indexEntity := range indexes {
		newIndexes = append(newIndexes, m.buildCreateIndexSql(keyName, indexEntity))
	}
	sort.Strings(newIndexes)
	for _, value := range newIndexes {
		createTableSql += fmt.Sprintf("  %s,\n", strings.TrimLeft(value, "ADD "))
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
	newIndexes = make([]string, 0)

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
	for keyName, indexEntity := range indexes {
		indexDB, has := indexesDB[keyName]
		if !has {
			newIndexes = append(newIndexes, m.buildCreateIndexSql(keyName, indexEntity))
			hasAlters = true
		} else {
			addIndexSqlEntity := m.buildCreateIndexSql(keyName, indexEntity)
			addIndexSqlDB := m.buildCreateIndexSql(keyName, indexDB)
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
	sort.Strings(droppedIndexes)
	for _, value := range droppedIndexes {
		alters = append(alters, fmt.Sprintf("    %s", value))
		comments = append(comments, "")
	}
	sort.Strings(newIndexes)
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

	if m.isTableEmpty(tableSchema) || (len(droppedColumns) == 0 && len(changedColumns) == 0) {
		alter = Alter{Sql: alterSql, Safe: true, Pool: tableSchema.MysqlPoolName}
	} else {
		alter = Alter{Sql: alterSql, Safe: false, Pool: tableSchema.MysqlPoolName}
	}
	has = true
	return has, alter, nil
}

func (m Mysql) isTableEmpty(tableSchema TableSchema) bool {
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

func (m Mysql) checkStruct(tableSchema TableSchema, t reflect.Type, indexes map[string]*index, prefix string) ([][2]string, error) {
	columns := make([][2]string, 0, t.NumField())
	max := t.NumField() - 1
	for i := 0; i <= max; i++ {
		if i == 0 && prefix == "" {
			continue
		}
		field := t.Field(i)
		fieldColumns, err := m.checkColumn(tableSchema, &field, indexes, prefix)
		if err != nil {
			return nil, err
		}
		if fieldColumns != nil {
			columns = append(columns, fieldColumns...)
		}
	}
	return columns, nil
}

func (m Mysql) checkColumn(tableSchema TableSchema, field *reflect.StructField, indexes map[string]*index, prefix string) ([][2]string, error) {
	var definition string
	var addNotNullIfNotSet bool
	addDefaultNullIfNullable := true
	defaultValue := "nil"
	var typeAsString = field.Type.String()
	columnName := prefix + field.Name

	attributes := tableSchema.Tags[columnName]

	_, has := attributes["ignore"]
	if has {
		return nil, nil
	}

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

	required, hasRequired := attributes["required"]
	isRequired := hasRequired && required == "true"

	var err error
	switch typeAsString {
	case "uint":
		definition, addNotNullIfNotSet, defaultValue = m.handleInt("int(10) unsigned")
	case "uint8":
		definition, addNotNullIfNotSet, defaultValue = m.handleInt("tinyint(3) unsigned")
	case "uint16":
		yearAttribute, _ := attributes["year"]
		if yearAttribute == "true" {
			if isRequired {
				return [][2]string{{columnName, fmt.Sprintf("`%s` year(4) NOT NULL DEFAULT '0000'", columnName)}}, nil
			} else {
				return [][2]string{{columnName, fmt.Sprintf("`%s` year(4) DEFAULT NULL", columnName)}}, nil
			}
		} else {
			definition, addNotNullIfNotSet, defaultValue = m.handleInt("smallint(5) unsigned")
		}
	case "uint32":
		mediumIntAttribute, _ := attributes["mediumint"]
		if mediumIntAttribute == "true" {
			definition, addNotNullIfNotSet, defaultValue = m.handleInt("mediumint(8) unsigned")
		} else {
			definition, addNotNullIfNotSet, defaultValue = m.handleInt("int(10) unsigned")
		}
	case "uint64":
		definition, addNotNullIfNotSet, defaultValue = m.handleInt("bigint(20) unsigned")
	case "int8":
		definition, addNotNullIfNotSet, defaultValue = m.handleInt("tinyint(4)")
	case "int16":
		definition, addNotNullIfNotSet, defaultValue = m.handleInt("smallint(6)")
	case "int32":
		mediumIntAttribute, _ := attributes["mediumint"]
		if mediumIntAttribute == "true" {
			definition, addNotNullIfNotSet, defaultValue = m.handleInt("mediumint(9)")
		} else {
			definition, addNotNullIfNotSet, defaultValue = m.handleInt("int(11)")
		}
	case "int64":
		definition, addNotNullIfNotSet, defaultValue = m.handleInt("bigint(20)")
	case "rune":
		definition, addNotNullIfNotSet, defaultValue = m.handleInt("int(11)")
	case "int":
		definition, addNotNullIfNotSet, defaultValue = m.handleInt("int(11)")
	case "bool":
		definition, addNotNullIfNotSet, defaultValue = m.handleInt("tinyint(1)")
	case "string", "[]string":
		definition, addNotNullIfNotSet, addDefaultNullIfNullable, defaultValue, err = m.handleString(attributes, false)
		if err != nil {
			return nil, err
		}
	case "interface {}", "[]uint64":
		definition, addNotNullIfNotSet, addDefaultNullIfNullable, defaultValue, err = m.handleString(attributes, true)
		if err != nil {
			return nil, err
		}
	case "float32":
		definition, addNotNullIfNotSet, defaultValue = m.handleFloat("float", attributes)
	case "float64":
		definition, addNotNullIfNotSet, defaultValue = m.handleFloat("double", attributes)
	case "time.Time":
		definition, addNotNullIfNotSet, addDefaultNullIfNullable, defaultValue = m.handleTime(attributes)
	case "[]uint8":
		definition = "blob"
		addDefaultNullIfNullable = false
	case "*orm.ReferenceOne":
		definition = m.handleReferenceOne(attributes)
		addNotNullIfNotSet = true
		addDefaultNullIfNullable = true
		defaultValue = "'0'"
	case "*orm.ReferenceMany":
		definition = m.handleReferenceMany(attributes)
		addNotNullIfNotSet = false
		addDefaultNullIfNullable = true
	case "*orm.CachedQuery":
		return nil, nil
	default:
		kind := field.Type.Kind().String()
		if kind == "struct" {
			structFields, err := m.checkStruct(tableSchema, field.Type, indexes, field.Name)
			if err != nil {
				return nil, err
			}
			return structFields, nil
		}
		return nil, fmt.Errorf("unsoported field type: %s %s", field.Name, field.Type.String())
	}
	isNotNull := false
	if addNotNullIfNotSet || isRequired {
		definition += " NOT NULL"
		isNotNull = true
	}
	if defaultValue != "nil" && columnName != "Id" && columnName != "ID" {
		definition += " DEFAULT " + defaultValue
	} else if !isNotNull && addDefaultNullIfNullable {
		definition += " DEFAULT NULL"
	}
	return [][2]string{{columnName, fmt.Sprintf("`%s` %s", columnName, definition)}}, nil
}

func (m Mysql) handleInt(definition string) (string, bool, string) {
	return definition, true, "'0'"
}

func (m Mysql) handleFloat(floatDefinition string, attributes map[string]string) (string, bool, string) {
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
	return definition, true, "'0'"
}

func (m Mysql) handleString(attributes map[string]string, forceMax bool) (string, bool, bool, string, error) {
	var definition string
	enum, hasEnum := attributes["enum"]
	if hasEnum {
		return m.handleSetEnum("enum", enum, attributes)
	}
	set, haSet := attributes["set"]
	if haSet {
		return m.handleSetEnum("set", set, attributes)
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
	defaultValue := "nil"
	required, hasRequired := attributes["required"]
	if hasRequired && required == "true" {
		defaultValue = "''"
	}
	return definition, false, addDefaultNullIfNullable, defaultValue, nil
}

func (m Mysql) handleSetEnum(fieldType string, attribute string, attributes map[string]string) (string, bool, bool, string, error) {
	enum, has := enums[attribute]
	if !has {
		return "", false, false, "", fmt.Errorf("unregistered enum %s", attribute)
	}
	values := make([]string, 0)
	for i := 0; i < enum.Type().NumField(); i++ {
		values = append(values, enum.Field(i).String())
	}

	var definition = fieldType + "("
	for key, value := range values {
		if key > 0 {
			definition += ","
		}
		definition += fmt.Sprintf("'%s'", value)
	}
	definition += ")"
	required, hasRequired := attributes["required"]
	defaultValue := "nil"
	if hasRequired && required == "true" {
		defaultValue = fmt.Sprintf("'%s'", values[0])
	}
	return definition, hasRequired && required == "true", true, defaultValue, nil
}

func (m Mysql) handleTime(attributes map[string]string) (string, bool, bool, string) {
	t, _ := attributes["time"]
	required, hasRequired := attributes["required"]
	isRequired := hasRequired && required == "true"
	defaultValue := "nil"
	if t == "true" {
		if isRequired {
			defaultValue = "'0001-01-01 00:00:00'"
		}
		return "datetime", isRequired, true, "nil"
	}
	if isRequired {
		defaultValue = "'0001-01-01'"
	}
	return "date", isRequired, true, defaultValue
}

func (m Mysql) handleReferenceOne(attributes map[string]string) string {
	reference, has := attributes["ref"]
	if !has {
		panic(fmt.Errorf("missing ref tag"))
	}
	typeAsString := GetEntityType(reference).Field(1).Type.String()
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

func (m Mysql) handleReferenceMany(attributes map[string]string) string {
	_, has := attributes["ref"]
	if !has {
		panic(fmt.Errorf("missing ref tag"))
	}
	return "varchar(5000)"
}

func (m Mysql) buildCreateIndexSql(keyName string, definition *index) string {
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
