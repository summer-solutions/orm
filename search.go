package orm

import (
	"database/sql"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
)

func searchIDsWithCount(skipFakeDelete bool, engine *Engine, where *Where, pager *Pager, entityType reflect.Type) (results []uint64, totalRows int, err error) {
	return searchIDs(skipFakeDelete, engine, where, pager, true, entityType)
}

func searchRow(skipFakeDelete bool, engine *Engine, where *Where, entity Entity, references []string) (bool, error) {
	orm := initIfNeeded(engine, entity)
	schema := orm.tableSchema
	whereQuery := where.String()
	if skipFakeDelete && schema.hasFakeDelete {
		whereQuery = fmt.Sprintf("`FakeDelete` = 0 AND %s", whereQuery)
	}
	/* #nosec */
	query := fmt.Sprintf("SELECT %s FROM `%s` WHERE %s LIMIT 1", schema.fieldsQuery, schema.tableName, whereQuery)

	pool := schema.GetMysql(engine)
	results, def, err := pool.Query(query, where.GetParameters()...)
	if err != nil {
		return false, err
	}
	defer def()
	if !results.Next() {
		err = results.Err()
		if err != nil {
			return false, err
		}
		return false, nil
	}

	columns, err := results.Columns()
	if err != nil {
		return false, err
	}
	count := len(columns)

	values := make([]sql.NullString, count)
	valuePointers := make([]interface{}, count)
	for i := range columns {
		valuePointers[i] = &values[i]
	}
	err = results.Scan(valuePointers...)
	if err != nil {
		return false, err
	}
	err = results.Err()
	if err != nil {
		return false, err
	}
	def()
	id, _ := strconv.ParseUint(values[0].String, 10, 64)
	finalValues := make([]string, count)
	for i, v := range values {
		finalValues[i] = v.String
	}
	err = fillFromDBRow(id, engine, finalValues[1:], entity)
	if err != nil {
		return false, err
	}
	if len(references) > 0 {
		err = warmUpReferences(engine, schema, entity.getORM().attributes.elem, references, false)
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func search(skipFakeDelete bool, engine *Engine, where *Where, pager *Pager, withCount bool, entities reflect.Value, references ...string) (int, error) {
	if pager == nil {
		pager = &Pager{CurrentPage: 1, PageSize: 50000}
	}
	entities.SetLen(0)
	entityType, has := getEntityTypeForSlice(engine.registry, entities.Type())
	if !has {
		return 0, EntityNotRegisteredError{Name: entities.String()}
	}
	schema := getTableSchema(engine.registry, entityType)
	whereQuery := where.String()
	if skipFakeDelete && schema.hasFakeDelete {
		whereQuery = fmt.Sprintf("`FakeDelete` = 0 AND %s", whereQuery)
	}
	/* #nosec */
	query := fmt.Sprintf("SELECT %s FROM `%s` WHERE %s %s", schema.fieldsQuery, schema.tableName, whereQuery,
		fmt.Sprintf("LIMIT %d,%d", (pager.CurrentPage-1)*pager.PageSize, pager.PageSize))
	pool := schema.GetMysql(engine)
	results, def, err := pool.Query(query, where.GetParameters()...)
	if err != nil {
		return 0, err
	}
	defer def()

	columns, err := results.Columns()
	if err != nil {
		return 0, err
	}
	count := len(columns)

	values := make([]string, count)
	valuePointers := make([]interface{}, count)

	valOrigin := entities
	val := valOrigin
	i := 0
	for results.Next() {
		for i := range columns {
			valuePointers[i] = &values[i]
		}
		err = results.Scan(valuePointers...)
		if err != nil {
			return 0, err
		}
		value := reflect.New(entityType)
		id, _ := strconv.ParseUint(values[0], 10, 64)
		err = fillFromDBRow(id, engine, values[1:], value.Interface().(Entity))
		if err != nil {
			return 0, err
		}
		val = reflect.Append(val, value)
		i++
	}
	err = results.Err()
	if err != nil {
		return 0, err
	}
	def()
	totalRows, err := getTotalRows(engine, withCount, pager, where, schema, i)
	if err != nil {
		return 0, err
	}
	if len(references) > 0 && i > 0 {
		err = warmUpReferences(engine, schema, val, references, true)
		if err != nil {
			return 0, err
		}
	}
	valOrigin.Set(val)
	return totalRows, nil
}

func searchOne(skipFakeDelete bool, engine *Engine, where *Where, entity Entity, references []string) (bool, error) {
	has, err := searchRow(skipFakeDelete, engine, where, entity, references)
	if err != nil {
		return false, err
	}
	return has, nil
}

func searchIDs(skipFakeDelete bool, engine *Engine, where *Where, pager *Pager, withCount bool, entityType reflect.Type) (ids []uint64, total int, err error) {
	schema := getTableSchema(engine.registry, entityType)
	if schema == nil {
		return nil, 0, EntityNotRegisteredError{Name: entityType.String()}
	}
	whereQuery := where.String()
	if skipFakeDelete && schema.hasFakeDelete {
		/* #nosec */
		whereQuery = fmt.Sprintf("`FakeDelete` = 0 AND %s", whereQuery)
	}
	/* #nosec */
	query := fmt.Sprintf("SELECT `ID` FROM `%s` WHERE %s %s", schema.tableName, whereQuery,
		fmt.Sprintf("LIMIT %d,%d", (pager.CurrentPage-1)*pager.PageSize, pager.PageSize))
	pool := schema.GetMysql(engine)
	results, def, err := pool.Query(query, where.GetParameters()...)
	if err != nil {
		return nil, 0, err
	}
	defer def()
	result := make([]uint64, 0, pager.GetPageSize())
	for results.Next() {
		var row uint64
		err = results.Scan(&row)
		if err != nil {
			return nil, 0, err
		}
		result = append(result, row)
	}
	err = results.Err()
	if err != nil {
		return nil, 0, err
	}
	def()
	totalRows, err := getTotalRows(engine, withCount, pager, where, schema, len(result))
	if err != nil {
		return nil, 0, err
	}
	return result, totalRows, nil
}

func getTotalRows(engine *Engine, withCount bool, pager *Pager, where *Where, schema *tableSchema, foundRows int) (int, error) {
	totalRows := 0
	if withCount {
		totalRows = foundRows
		if totalRows == pager.GetPageSize() || (foundRows == 0 && pager.CurrentPage > 1) {
			/* #nosec */
			query := fmt.Sprintf("SELECT count(1) FROM `%s` WHERE %s", schema.tableName, where)
			var foundTotal string
			pool := schema.GetMysql(engine)
			err := pool.QueryRow(query, where.GetParameters()...).Scan(&foundTotal)
			if err != nil {
				return 0, err
			}
			totalRows, _ = strconv.Atoi(foundTotal)
		} else {
			totalRows += (pager.GetCurrentPage() - 1) * pager.GetPageSize()
		}
	}
	return totalRows, nil
}

func fillFromDBRow(id uint64, engine *Engine, data []string, entity Entity) error {
	orm := initIfNeeded(engine, entity)
	elem := orm.attributes.elem
	orm.attributes.idElem.SetUint(id)
	_, err := fillStruct(engine, 0, data, orm.tableSchema.fields, elem)
	if err != nil {
		return err
	}
	orm.dBData["ID"] = id
	orm.attributes.loaded = true
	for key, column := range orm.tableSchema.columnNames[1:] {
		orm.dBData[column] = data[key]
	}
	return nil
}

func fillStruct(engine *Engine, index uint16, data []string, fields *tableFields, value reflect.Value) (uint16, error) {
	skip := 1
	if fields.prefix != "" {
		skip = -1
	}
	for _, i := range fields.uintegers {
		if i == skip {
			continue
		}
		integer, _ := strconv.ParseUint(data[index], 10, 64)
		value.Field(i).SetUint(integer)
		index++
	}
	for _, i := range fields.integers {
		integer, _ := strconv.ParseInt(data[index], 10, 64)
		value.Field(i).SetInt(integer)
		index++
	}
	for _, i := range fields.strings {
		value.Field(i).SetString(data[index])
		index++
	}
	for _, i := range fields.sliceStrings {
		field := value.Field(i)
		if data[index] != "" {
			var values = strings.Split(data[index], ",")
			var length = len(values)
			slice := reflect.MakeSlice(field.Type(), length, length)
			for key, value := range values {
				slice.Index(key).SetString(value)
			}
			field.Set(slice)
		} else {
			field.Set(reflect.Zero(field.Type()))
		}
		index++
	}
	for _, i := range fields.bytes {
		bytes := data[index]
		field := value.Field(i)
		if bytes != "" {
			field.SetBytes([]byte(bytes))
		} else {
			field.Set(reflect.Zero(field.Type()))
		}
		index++
	}
	if fields.fakeDelete > 0 {
		val := true
		if data[index] == "0" {
			val = false
		}
		value.Field(fields.fakeDelete).SetBool(val)
		index++
	}
	for _, i := range fields.booleans {
		value.Field(i).SetBool(data[index] == "1")
		index++
	}
	for _, i := range fields.floats {
		float, _ := strconv.ParseFloat(data[index], 64)
		value.Field(i).SetFloat(float)
		index++
	}
	for _, i := range fields.timesNullable {
		field := value.Field(i)
		if data[index] == "" {
			field.Set(reflect.Zero(field.Type()))
		} else {
			layout := "2006-01-02"
			if len(data[index]) == 19 {
				layout += " 15:04:05"
			}
			value, _ := time.Parse(layout, data[index])
			field.Set(reflect.ValueOf(&value))
		}
		index++
	}
	for _, i := range fields.times {
		field := value.Field(i)
		layout := "2006-01-02"
		if len(data[index]) == 19 {
			layout += " 15:04:05"
		}
		val, _ := time.Parse(layout, data[index])
		field.Set(reflect.ValueOf(val))
		index++
	}
	for _, i := range fields.jsons {
		field := value.Field(i)
		if data[index] != "" {
			var f interface{}
			err := jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal([]byte(data[index]), &f)
			if err != nil {
				return 0, err
			}
			field.Set(reflect.ValueOf(f))
		} else {
			field.Set(reflect.Zero(field.Type()))
		}
		index++
	}
	for k, i := range fields.refs {
		field := value.Field(i)
		integer, _ := strconv.ParseUint(data[index], 10, 64)
		refType := fields.refsTypes[k]
		if integer > 0 {
			n := reflect.New(refType.Elem())
			orm := initIfNeeded(engine, n.Interface().(Entity))
			orm.attributes.idElem.SetUint(integer)
			field.Set(n)
		} else {
			field.Set(reflect.Zero(refType))
		}
		index++
	}
	for i, subFields := range fields.structs {
		field := value.Field(i)
		newVal := reflect.New(field.Type())
		value := newVal.Elem()
		newIndex, err := fillStruct(engine, index, data, subFields, value)
		if err != nil {
			return 0, err
		}
		field.Set(value)
		index = newIndex
	}
	return index, nil
}

func getEntityTypeForSlice(registry *validatedRegistry, sliceType reflect.Type) (reflect.Type, bool) {
	name := strings.Trim(sliceType.String(), "*[]")
	e, has := registry.entities[name]
	return e, has
}
