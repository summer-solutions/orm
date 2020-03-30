package orm

import (
	"reflect"
)

func isDirty(value reflect.Value) (is bool, bind map[string]interface{}, err error) {
	t := value.Type()
	ormField := value.Field(0).Interface().(*ORM)
	if ormField.dBData["_delete"] == true {
		return true, nil, nil
	}
	bind, err = createBind(ormField.tableSchema, t, value, ormField.dBData, "")
	if err != nil {
		return false, nil, err
	}
	is = value.Field(1).Uint() == 0 || len(bind) > 0
	return is, bind, nil
}
