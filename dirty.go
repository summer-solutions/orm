package orm

import (
	"reflect"
)

func isDirty(value reflect.Value) (is bool, bind map[string]interface{}, err error) {
	id := value.Field(1).Uint()
	t := value.Type()
	ormField := value.Field(0).Interface().(*ORM)
	if ormField.dBData["_delete"] == true {
		return true, nil, nil
	}
	bind, err = createBind(true, id, ormField.tableSchema, t, value, ormField.dBData, "")
	if err != nil {
		return false, nil, err
	}
	is = id == 0 || len(bind) > 0
	return is, bind, nil
}
