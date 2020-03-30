package orm

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func searchIdsWithCount(engine *Engine, where *Where, pager *Pager, entityType reflect.Type) (results []uint64, totalRows int, err error) {
	return searchIds(engine, where, pager, true, entityType)
}

func searchRow(engine *Engine, where *Where, value reflect.Value) (bool, error) {

	entityType := value.Elem().Type()
	schema := getTableSchema(engine.config, entityType)
	var fieldsList = buildFieldList(engine.config, entityType, "")
	query := fmt.Sprintf("SELECT %s FROM `%s` WHERE %s LIMIT 1", fieldsList, schema.TableName, where)

	results, err := schema.GetMysql(engine).Query(query, where.GetParameters()...)
	if err != nil {
		return false, err
	}
	if !results.Next() {
		return false, nil
	}

	columns, err := results.Columns()
	if err != nil {
		return false, err
	}
	count := len(columns)

	values := make([]string, count)
	valuePointers := make([]interface{}, count)
	for i := range columns {
		valuePointers[i] = &values[i]
	}
	err = results.Scan(valuePointers...)
	if err != nil {
		return false, err
	}

	err = fillFromDBRow(engine, values, value, entityType)
	if err != nil {
		return false, err
	}
	return true, nil
}

func search(engine *Engine, where *Where, pager *Pager, withCount bool, entities reflect.Value, references ...string) (int, error) {

	if pager == nil {
		pager = &Pager{CurrentPage: 1, PageSize: 50000}
	}
	entities.SetLen(0)
	entityType := getEntityTypeForSlice(engine.config, entities.Type())
	schema := getTableSchema(engine.config, entityType)

	var fieldsList = buildFieldList(engine.config, entityType, "")
	query := fmt.Sprintf("SELECT %s FROM `%s` WHERE %s %s", fieldsList, schema.TableName, where,
		fmt.Sprintf("LIMIT %d,%d", (pager.CurrentPage-1)*pager.PageSize, pager.PageSize))
	results, err := schema.GetMysql(engine).Query(query, where.GetParameters()...)
	if err != nil {
		return 0, err
	}

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
			panic(err.Error())
		}
		value := reflect.New(entityType)
		err = fillFromDBRow(engine, values, value, entityType)
		if err != nil {
			return 0, err
		}
		val = reflect.Append(val, value)
		i++
	}
	totalRows := getTotalRows(engine, withCount, pager, where, schema, i)
	if len(references) > 0 && i > 0 {
		err = warmUpReferences(engine, schema, val, references, true)
		if err != nil {
			return 0, err
		}
	}
	valOrigin.Set(val)
	return totalRows, nil
}

func searchOne(engine *Engine, where *Where, entity interface{}) (bool, error) {

	value := reflect.ValueOf(entity)
	has, err := searchRow(engine, where, value)
	if err != nil {
		return false, err
	}
	return has, nil
}

func searchIds(engine *Engine, where *Where, pager *Pager, withCount bool, entityType reflect.Type) (ids []uint64, total int, err error) {
	schema := getTableSchema(engine.config, entityType)
	query := fmt.Sprintf("SELECT `Id` FROM `%s` WHERE %s %s", schema.TableName, where,
		fmt.Sprintf("LIMIT %d,%d", (pager.CurrentPage-1)*pager.PageSize, pager.PageSize))
	results, err := schema.GetMysql(engine).Query(query, where.GetParameters()...)
	if err != nil {
		return nil, 0, err
	}
	result := make([]uint64, 0, pager.GetPageSize())
	for results.Next() {
		var row uint64
		err = results.Scan(&row)
		if err != nil {
			panic(err.Error())
		}
		result = append(result, row)
	}
	totalRows := getTotalRows(engine, withCount, pager, where, schema, len(result))
	return result, totalRows, nil
}

func getTotalRows(engine *Engine, withCount bool, pager *Pager, where *Where, schema *TableSchema, foundRows int) int {
	totalRows := 0
	if withCount {
		totalRows = foundRows
		if totalRows == pager.GetPageSize() {
			query := fmt.Sprintf("SELECT count(1) FROM `%s` WHERE %s", schema.TableName, where)
			var foundTotal string
			err := schema.GetMysql(engine).QueryRow(query, where.GetParameters()...).Scan(&foundTotal)
			if err != nil {
				panic(err.Error())
			}
			totalRows, _ = strconv.Atoi(foundTotal)
		} else {
			totalRows += (pager.GetCurrentPage() - 1) * pager.GetPageSize()
		}
	}
	return totalRows
}

func fillFromDBRow(engine *Engine, data []string, value reflect.Value, entityType reflect.Type) error {
	orm := engine.initIfNeeded(value)
	elem := value.Elem()
	fillStruct(engine.config, 0, data, entityType, elem, "")
	orm.dBData["Id"] = data[0]

	_, bind, err := isDirty(elem)
	if err != nil {
		return err
	}
	for key, value := range bind {
		orm.dBData[key] = value
	}
	return nil
}

func fillStruct(config *Config, index uint16, data []string, t reflect.Type, value reflect.Value, prefix string) uint16 {

	for i := 0; i < t.NumField(); i++ {

		if index == 0 && i == 0 {
			continue
		}

		field := value.Field(i)
		name := prefix + t.Field(i).Name

		tags := getTableSchema(config, t).Tags[name]
		_, has := tags["ignore"]
		if has {
			continue
		}

		fieldType := field.Type().String()
		switch fieldType {
		case "uint":
			integer, _ := strconv.ParseUint(data[index], 10, 32)
			field.SetUint(integer)
		case "uint8":
			integer, _ := strconv.ParseUint(data[index], 10, 8)
			field.SetUint(integer)
		case "uint16":
			integer, _ := strconv.ParseUint(data[index], 10, 16)
			field.SetUint(integer)
		case "uint32":
			integer, _ := strconv.ParseUint(data[index], 10, 32)
			field.SetUint(integer)
		case "uint64":
			integer, _ := strconv.ParseUint(data[index], 10, 64)
			field.SetUint(integer)
		case "int":
			integer, _ := strconv.ParseInt(data[index], 10, 32)
			field.SetInt(integer)
		case "int8":
			integer, _ := strconv.ParseInt(data[index], 10, 8)
			field.SetInt(integer)
		case "int16":
			integer, _ := strconv.ParseInt(data[index], 10, 16)
			field.SetInt(integer)
		case "int32":
			integer, _ := strconv.ParseInt(data[index], 10, 32)
			field.SetInt(integer)
		case "int64":
			integer, _ := strconv.ParseInt(data[index], 10, 64)
			field.SetInt(integer)
		case "string":
			field.SetString(data[index])
		case "[]string":
			if data[index] != "" {
				var values = strings.Split(data[index], ",")
				var length = len(values)
				slice := reflect.MakeSlice(field.Type(), length, length)
				for key, value := range values {
					slice.Index(key).SetString(value)
				}
				field.Set(slice)
			}
		case "[]uint64":
			if data[index] != "" {
				var values = strings.Split(data[index], " ")
				var length = len(values)
				slice := reflect.MakeSlice(field.Type(), length, length)
				for key, value := range values {
					integer, _ := strconv.ParseUint(value, 10, 64)
					slice.Index(key).SetUint(integer)
				}
				field.Set(slice)
			}
		case "[]uint8":
			bytes := data[index]
			if bytes != "" {
				field.SetBytes([]byte(bytes))
			}
		case "bool":
			val := false
			if data[index] == "1" {
				val = true
			}
			field.SetBool(val)
		case "float32":
			float, _ := strconv.ParseFloat(data[index], 32)
			field.SetFloat(float)
		case "float64":
			float, _ := strconv.ParseFloat(data[index], 64)
			field.SetFloat(float)
		case "time.Time":
			layout := "2006-01-02"
			if len(data[index]) == 19 {
				layout += " 15:04:05"
			}
			value, _ := time.Parse(layout, data[index])
			field.Set(reflect.ValueOf(value))
		case "*orm.ReferenceOne":
			integer, _ := strconv.ParseUint(data[index], 10, 64)
			field.Interface().(*ReferenceOne).Id = integer
		case "*orm.CachedQuery":
			continue
		case "interface {}":
			if data[index] != "" {
				var f interface{}
				err := json.Unmarshal([]byte(data[index]), &f)
				if err != nil {
					panic(fmt.Errorf("invalid json: %s", data[index]))
				}
				field.Set(reflect.ValueOf(f))
			}
		default:
			if field.Kind().String() == "struct" {
				newVal := reflect.New(field.Type())
				value := newVal.Elem()
				index = fillStruct(config, index, data, field.Type(), value, name)
				field.Set(value)
				continue
			}
			panic(fmt.Errorf("unsoported field type: %s", field.Type().String()))
		}
		index++
	}
	return index
}

func buildFieldList(config *Config, t reflect.Type, prefix string) string {
	fieldsList := ""
	for i := 0; i < t.NumField(); i++ {
		var columnNameRaw string
		field := t.Field(i)
		tags := getTableSchema(config, t).Tags[field.Name]
		_, has := tags["ignore"]
		if has {
			continue
		}
		if prefix == "" && (strings.ToLower(field.Name) == "id" || field.Name == "Orm") {
			continue
		}
		if field.Type.String() == "*orm.CachedQuery" {
			continue
		}
		switch field.Type.String() {
		case "string", "[]string", "[]uint8", "interface {}", "uint16", "*orm.ReferenceOne", "time.Time":
			columnNameRaw = prefix + t.Field(i).Name
			fieldsList += fmt.Sprintf(",IFNULL(`%s`,'')", columnNameRaw)
		default:
			if field.Type.Kind().String() == "struct" {
				fieldsList += buildFieldList(config, field.Type, field.Name)
			} else {
				columnNameRaw = prefix + t.Field(i).Name
				fieldsList += fmt.Sprintf(",`%s`", columnNameRaw)
			}
		}
	}
	if prefix == "" {
		fieldsList = "`Id`" + fieldsList
	}
	return fieldsList
}

func getEntityTypeForSlice(config *Config, sliceType reflect.Type) reflect.Type {
	name := strings.Trim(sliceType.String(), "*[]")
	return config.GetEntityType(name)
}
