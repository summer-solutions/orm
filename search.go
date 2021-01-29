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

func searchIDsWithCount(skipFakeDelete bool, engine *Engine, where *Where, pager *Pager, entityType reflect.Type) (results []uint64, totalRows int) {
	return searchIDs(skipFakeDelete, engine, where, pager, true, entityType)
}

func searchRow(skipFakeDelete bool, engine *Engine, where *Where, entity Entity, references []string) bool {
	orm := initIfNeeded(engine, entity)
	schema := orm.tableSchema
	whereQuery := where.String()
	if skipFakeDelete && schema.hasFakeDelete {
		whereQuery = fmt.Sprintf("`FakeDelete` = 0 AND %s", whereQuery)
	}
	/* #nosec */
	query := fmt.Sprintf("SELECT %s FROM `%s` WHERE %s LIMIT 1", schema.fieldsQuery, schema.tableName, whereQuery)

	pool := schema.GetMysql(engine)
	results, def := pool.Query(query, where.GetParameters()...)
	defer def()
	if !results.Next() {
		return false
	}
	count := len(schema.columnNames)

	values := make([]sql.NullString, count)
	valuePointers := make([]interface{}, count)
	for i := 0; i < count; i++ {
		valuePointers[i] = &values[i]
	}
	results.Scan(valuePointers...)
	def()
	id := uint64(0)
	if values[0].Valid {
		id, _ = strconv.ParseUint(values[0].String, 10, 64)
	}

	finalValues := make([]string, count)
	for i, v := range values {
		if v.Valid {
			finalValues[i] = v.String
		} else {
			finalValues[i] = "nil"
		}
	}
	fillFromDBRow(id, engine, finalValues[1:], entity, true)
	if len(references) > 0 {
		warmUpReferences(engine, schema, entity.getORM().elem, references, false)
	}
	return true
}

func search(skipFakeDelete bool, engine *Engine, where *Where, pager *Pager, withCount bool, entities reflect.Value, references ...string) int {
	if pager == nil {
		pager = NewPager(1, 50000)
	}
	entities.SetLen(0)
	entityType, has, name := getEntityTypeForSlice(engine.registry, entities.Type())
	if !has {
		panic(fmt.Errorf("entity '%s' is not registered", name))
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
	results, def := pool.Query(query, where.GetParameters()...)
	defer def()

	count := len(schema.columnNames)

	values := make([]sql.NullString, count)
	valuePointers := make([]interface{}, count)
	for i := 0; i < count; i++ {
		valuePointers[i] = &values[i]
	}

	valOrigin := entities
	val := valOrigin
	i := 0
	for results.Next() {
		results.Scan(valuePointers...)
		finalValues := make([]string, count)
		for i, v := range values {
			if v.Valid {
				finalValues[i] = v.String
			} else {
				finalValues[i] = "nil"
			}
		}
		value := reflect.New(entityType)
		id, _ := strconv.ParseUint(finalValues[0], 10, 64)
		fillFromDBRow(id, engine, finalValues[1:], value.Interface().(Entity), true)
		val = reflect.Append(val, value)
		i++
	}
	def()
	totalRows := getTotalRows(engine, withCount, pager, where, schema, i)
	if len(references) > 0 && i > 0 {
		warmUpReferences(engine, schema, val, references, true)
	}
	valOrigin.Set(val)
	return totalRows
}

func searchOne(skipFakeDelete bool, engine *Engine, where *Where, entity Entity, references []string) bool {
	return searchRow(skipFakeDelete, engine, where, entity, references)
}

func searchIDs(skipFakeDelete bool, engine *Engine, where *Where, pager *Pager, withCount bool, entityType reflect.Type) (ids []uint64, total int) {
	if pager == nil {
		pager = NewPager(1, 50000)
	}
	schema := getTableSchema(engine.registry, entityType)
	whereQuery := where.String()
	if skipFakeDelete && schema.hasFakeDelete {
		/* #nosec */
		whereQuery = fmt.Sprintf("`FakeDelete` = 0 AND %s", whereQuery)
	}
	/* #nosec */
	query := fmt.Sprintf("SELECT `ID` FROM `%s` WHERE %s %s", schema.tableName, whereQuery,
		fmt.Sprintf("LIMIT %d,%d", (pager.CurrentPage-1)*pager.PageSize, pager.PageSize))
	pool := schema.GetMysql(engine)
	results, def := pool.Query(query, where.GetParameters()...)
	defer def()
	result := make([]uint64, 0, pager.GetPageSize())
	for results.Next() {
		var row uint64
		results.Scan(&row)
		result = append(result, row)
	}
	def()
	totalRows := getTotalRows(engine, withCount, pager, where, schema, len(result))
	return result, totalRows
}

func getTotalRows(engine *Engine, withCount bool, pager *Pager, where *Where, schema *tableSchema, foundRows int) int {
	totalRows := 0
	if withCount {
		totalRows = foundRows
		if totalRows == pager.GetPageSize() || (foundRows == 0 && pager.CurrentPage > 1) {
			/* #nosec */
			query := fmt.Sprintf("SELECT count(1) FROM `%s` WHERE %s", schema.tableName, where)
			var foundTotal string
			pool := schema.GetMysql(engine)
			pool.QueryRow(NewWhere(query, where.GetParameters()...), &foundTotal)
			totalRows, _ = strconv.Atoi(foundTotal)
		} else {
			totalRows += (pager.GetCurrentPage() - 1) * pager.GetPageSize()
		}
	}
	return totalRows
}

func fillFromDBRow(id uint64, engine *Engine, data []string, entity Entity, fillDataLoader bool) {
	orm := initIfNeeded(engine, entity)
	elem := orm.elem
	orm.idElem.SetUint(id)
	_ = fillStruct(engine, 0, data, orm.tableSchema.fields, elem)
	orm.dBData["ID"] = id
	orm.loaded = true
	for key, column := range orm.tableSchema.columnNames[1:] {
		orm.dBData[column] = data[key]
	}
	if !fillDataLoader {
		return
	}
	schema := entity.getORM().tableSchema
	if !schema.hasLocalCache && engine.dataLoader != nil {
		engine.dataLoader.Prime(schema, id, data)
	}
}

func convertStringToUint(value string) uint64 {
	v, _ := strconv.ParseUint(value, 10, 64)
	return v
}

func convertStringToInt(value string) int64 {
	v, _ := strconv.ParseInt(value, 10, 64)
	return v
}

func fillStruct(engine *Engine, index uint16, data []string, fields *tableFields, value reflect.Value) uint16 {
	skip := 1
	if fields.prefix != "" {
		skip = -1
	}
	for _, i := range fields.uintegers {
		if i == skip {
			continue
		}
		value.Field(i).SetUint(convertStringToUint(data[index]))
		index++
	}
	for _, i := range fields.uintegersNullable {
		field := value.Field(i)
		if data[index] == "nil" {
			field := value.Field(i)
			field.Set(reflect.Zero(field.Type()))
		} else {
			val := convertStringToUint(data[index])
			switch field.Type().String() {
			case "*uint":
				v := uint(val)
				field.Set(reflect.ValueOf(&v))
			case "*uint8":
				v := uint8(val)
				field.Set(reflect.ValueOf(&v))
			case "*uint16":
				v := uint16(val)
				field.Set(reflect.ValueOf(&v))
			case "*uint32":
				v := uint32(val)
				field.Set(reflect.ValueOf(&v))
			default:
				field.Set(reflect.ValueOf(&val))
			}
		}
		index++
	}
	for _, i := range fields.integers {
		value.Field(i).SetInt(convertStringToInt(data[index]))
		index++
	}
	for _, i := range fields.integersNullable {
		field := value.Field(i)
		if data[index] == "nil" {
			field.Set(reflect.Zero(field.Type()))
		} else {
			val := convertStringToInt(data[index])
			switch field.Type().String() {
			case "*int":
				v := int(val)
				field.Set(reflect.ValueOf(&v))
			case "*int8":
				v := int8(val)
				field.Set(reflect.ValueOf(&v))
			case "*int16":
				v := int16(val)
				field.Set(reflect.ValueOf(&v))
			case "*int32":
				v := int32(val)
				field.Set(reflect.ValueOf(&v))
			default:
				field.Set(reflect.ValueOf(&val))
			}
		}
		index++
	}
	for _, i := range fields.strings {
		field := value.Field(i)
		if data[index] == "nil" {
			field.SetString("")
		} else {
			field.SetString(data[index])
		}
		index++
	}
	for _, i := range fields.sliceStrings {
		field := value.Field(i)
		if data[index] != "nil" {
			if data[index] == "" {
				field.Set(reflect.MakeSlice(field.Type(), 0, 0))
			} else {
				var values = strings.Split(data[index], ",")
				var length = len(values)
				slice := reflect.MakeSlice(field.Type(), length, length)
				for key, value := range values {
					slice.Index(key).SetString(value)
				}
				field.Set(slice)
			}
		} else {
			field.Set(reflect.Zero(field.Type()))
		}
		index++
	}
	for _, i := range fields.bytes {
		bytes := data[index]
		field := value.Field(i)
		if bytes != "nil" {
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
	for _, i := range fields.booleansNullable {
		field := value.Field(i)
		if data[index] == "nil" {
			field.Set(reflect.Zero(field.Type()))
		} else {
			v := data[index] == "1"
			field.Set(reflect.ValueOf(&v))
		}
		index++
	}
	for _, i := range fields.floats {
		float, _ := strconv.ParseFloat(data[index], 64)
		value.Field(i).SetFloat(float)
		index++
	}
	for _, i := range fields.floatsNullable {
		field := value.Field(i)
		if data[index] == "nil" {
			field.Set(reflect.Zero(field.Type()))
		} else {
			val, _ := strconv.ParseFloat(data[index], 64)
			switch field.Type().String() {
			case "*float32":
				v := float32(val)
				field.Set(reflect.ValueOf(&v))
			default:
				field.Set(reflect.ValueOf(&val))
			}
		}
		index++
	}
	for _, i := range fields.timesNullable {
		field := value.Field(i)
		if data[index] == "nil" {
			field.Set(reflect.Zero(field.Type()))
		} else {
			layout := "2006-01-02"
			if len(data[index]) == 19 {
				layout += " 15:04:05"
			}
			value, _ := time.ParseInLocation(layout, data[index], time.Local)
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
		val, _ := time.ParseInLocation(layout, data[index], time.Local)
		field.Set(reflect.ValueOf(val))
		index++
	}
	for _, i := range fields.jsons {
		field := value.Field(i)
		if data[index] != "nil" {
			f := reflect.New(field.Type()).Interface()
			_ = jsoniter.ConfigFastest.Unmarshal([]byte(data[index]), f)
			field.Set(reflect.ValueOf(f).Elem())
		} else {
			field.Set(reflect.Zero(field.Type()))
		}
		index++
	}
	for k, i := range fields.refs {
		field := value.Field(i)
		integer := uint64(0)
		if data[index] != "nil" {
			integer, _ = strconv.ParseUint(data[index], 10, 64)
		}
		refType := fields.refsTypes[k]
		if integer > 0 {
			n := reflect.New(refType.Elem())
			orm := initIfNeeded(engine, n.Interface().(Entity))
			orm.dBData["ID"] = integer
			orm.idElem.SetUint(integer)
			field.Set(n)
		} else {
			field.Set(reflect.Zero(refType))
		}
		index++
	}
	for k, i := range fields.refsMany {
		field := value.Field(i)
		var f []uint64
		length := 0
		if data[index] != "nil" {
			f = make([]uint64, 0)
			_ = jsoniter.ConfigFastest.Unmarshal([]byte(data[index]), &f)
			length = len(f)
		}
		refType := fields.refsManyTypes[k]
		slice := reflect.MakeSlice(reflect.SliceOf(refType), length, length)
		if f != nil {
			for i, id := range f {
				n := reflect.New(refType.Elem())
				orm := initIfNeeded(engine, n.Interface().(Entity))
				orm.dBData["ID"] = id
				orm.idElem.SetUint(id)
				slice.Index(i).Set(n)
			}
			field.Set(slice)
		} else {
			field.Set(reflect.Zero(slice.Type()))
		}
		index++
	}
	for i, subFields := range fields.structs {
		field := value.Field(i)
		newVal := reflect.New(field.Type())
		value := newVal.Elem()
		newIndex := fillStruct(engine, index, data, subFields, value)
		field.Set(value)
		index = newIndex
	}
	return index
}

func getEntityTypeForSlice(registry *validatedRegistry, sliceType reflect.Type) (reflect.Type, bool, string) {
	name := sliceType.String()
	if name[0] == 42 {
		name = name[1:]
	}
	if name[0] == 91 {
		name = name[3:]
	}
	e, has := registry.entities[name]
	return e, has, name
}
