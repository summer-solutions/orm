package orm

import "reflect"

func GetReference(entity interface{}, field string) interface{} {
	id := reflect.ValueOf(entity).FieldByName(field).Uint()
	if id == 0 {
		return nil
	}
	schema := GetTableSchema(reflect.TypeOf(entity).String())
	tName := schema.tags[field]["ref"]
	return GetById(id, tName)
}
