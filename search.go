package orm

import (
	"database/sql"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"reflect"
	"strconv"
	"strings"
	"time"
)

func SearchWithCount(where Where, pager Pager, entities interface{}) (totalRows int) {
	return search(where, pager, true, reflect.ValueOf(entities).Elem())
}

func Search(where Where, pager Pager, entities interface{}) {
	search(where, pager, false, reflect.ValueOf(entities).Elem())
}

func SearchIdsWithCount(where Where, pager Pager, entity interface{}) (results []uint64, totalRows int) {
	return searchIdsWithCount(where, pager, reflect.TypeOf(entity))
}

func SearchIds(where Where, pager Pager, entity interface{}) []uint64 {
	results, _ := searchIds(where, pager, false, reflect.TypeOf(entity))
	return results
}

func SearchOne(where Where, entity interface{}) bool {

	value := reflect.Indirect(reflect.ValueOf(entity))
	entityType := value.Type()
	has := searchRow(where, entityType, value)
	initIfNeeded(value, entity)
	return has
}

func searchIdsWithCount(where Where, pager Pager, entityType reflect.Type) (results []uint64, totalRows int) {
	return searchIds(where, pager, true, entityType)
}

func searchRow(where Where, entityType reflect.Type, value reflect.Value) bool {

	schema := getTableSchema(entityType)
	var fieldsList = buildFieldList(entityType, "")
	query := fmt.Sprintf("SELECT CONCAT_WS('|', %s) FROM `%s` WHERE %s LIMIT 1", fieldsList, schema.TableName, where)
	var row string
	err := schema.GetMysql().QueryRow(query, where.GetParameters()...).Scan(&row)
	if err != nil {
		if err == sql.ErrNoRows {
			return false
		}
		panic(err.Error())
	}

	fillFromDBRow(row, value, entityType)
	return true
}

func search(where Where, pager Pager, withCount bool, entities reflect.Value) int {

	entityType := getEntityTypeForSlice(entities.Type())
	schema := getTableSchema(entityType)

	var fieldsList = buildFieldList(entityType, "")
	query := fmt.Sprintf("SELECT CONCAT_WS('|', %s) FROM `%s` WHERE %s %s", fieldsList, schema.TableName, where, pager.String())
	results, err := schema.GetMysql().Query(query, where.GetParameters()...)
	if err != nil {
		panic(err.Error())
	}
	valOrigin := entities
	val := valOrigin
	i := 0
	for results.Next() {
		var row string
		err = results.Scan(&row)
		if err != nil {
			panic(err.Error())
		}
		value := reflect.New(entityType).Elem()
		fillFromDBRow(row, value, entityType)
		e := value.Interface()
		val = reflect.Append(val, reflect.ValueOf(e))
		initIfNeeded(value, &e)
		i++
	}
	totalRows := getTotalRows(withCount, pager, where, schema, i)
	valOrigin.Set(val)
	return totalRows
}

func searchIds(where Where, pager Pager, withCount bool, entityType reflect.Type) ([]uint64, int) {
	schema := getTableSchema(entityType)
	query := fmt.Sprintf("SELECT `Id` FROM `%s` WHERE %s %s", schema.TableName, where, pager.String())
	results, err := schema.GetMysql().Query(query, where.GetParameters()...)
	if err != nil {
		panic(err.Error())
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
	totalRows := getTotalRows(withCount, pager, where, schema, len(result))
	return result, totalRows
}

func getTotalRows(withCount bool, pager Pager, where Where, schema *TableSchema, foundRows int) int {
	totalRows := 0
	if withCount {
		totalRows = foundRows
		if totalRows == pager.GetPageSize() {
			query := fmt.Sprintf("SELECT count(1) FROM `%s` WHERE %s", schema.TableName, where)
			var foundTotal string
			err := schema.GetMysql().QueryRow(query, where.GetParameters()...).Scan(&foundTotal)
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

func fillFromDBRow(row string, value reflect.Value, entityType reflect.Type) {
	data := strings.Split(row, "|")

	e := value.Interface()
	orm := initIfNeeded(value, &e)
	fillStruct(0, data, entityType, value, "")
	orm.dBData["Id"] = data[0]

	_, bind := orm.isDirty(value)
	for key, value := range bind {
		orm.dBData[key] = value
	}
}

func fillStruct(index uint16, data []string, t reflect.Type, value reflect.Value, prefix string) uint16 {

	bind := make(map[string]interface{})
	for i := 0; i < t.NumField(); i++ {

		if index == 0 && i == 0 {
			continue
		}

		field := value.Field(i)
		name := prefix + t.Field(i).Name

		fieldType := field.Type().String()
		switch fieldType {
		case "uint":
			bind[name] = data[index]
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
		case "*orm.ReferenceMany":
			ids := data[index]
			if ids == "" {
				field.Interface().(*ReferenceMany).Ids = nil
			} else {
				val := strings.Split(ids, " ")
				idsAsInt := make([]uint64, len(val))
				for k, v := range val {
					idsAsInt[k], _ = strconv.ParseUint(v, 10, 64)
				}
				field.Interface().(*ReferenceMany).Ids = idsAsInt
			}
		case "*orm.CachedQuery":
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
				index = fillStruct(index, data, field.Type(), value, name)
				field.Set(value)
				continue
			}
			panic(fmt.Errorf("unsoported field type: %s", field.Type().String()))
		}
		index++
	}
	return index
}

func buildFieldList(t reflect.Type, prefix string) string {
	fieldsList := ""
	for i := 0; i < t.NumField(); i++ {
		var columnNameRaw string
		field := t.Field(i)
		if prefix == "" && (field.Name == "Id" || field.Name == "Orm") {
			continue
		}
		if field.Type.String() == "*orm.CachedQuery" {
			continue
		}
		switch field.Type.String() {
		case "string", "[]string", "interface {}", "*orm.ReferenceMany":
			columnNameRaw = prefix + t.Field(i).Name
			fieldsList += fmt.Sprintf(",IFNULL(`%s`,'')", columnNameRaw)
		default:
			if field.Type.Kind().String() == "struct" && field.Type.String() != "time.Time" {
				fieldsList += buildFieldList(field.Type, field.Name)
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

func getEntityTypeForSlice(sliceType reflect.Type) reflect.Type {
	name := strings.Trim(sliceType.String(), "*[]")
	return getEntityType(name)
}
