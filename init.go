package orm

import (
	"reflect"
)

func initIfNeeded(engine *Engine, value reflect.Value, withReferences bool) *ORM {
	elem := value.Elem()
	address := elem.Field(0).Addr()
	orm := address.Interface().(*ORM)
	if orm.dBData == nil {
		tableSchema := getTableSchema(engine.registry, elem.Type())
		orm.engine = engine
		orm.dBData = make(map[string]interface{})
		orm.elem = elem
		orm.value = value
		orm.tableSchema = tableSchema
		if withReferences {
			for _, code := range tableSchema.refOne {
				reference := tableSchema.Tags[code]["ref"]
				t, _ := engine.registry.entities[reference]
				n := reflect.New(t)
				initIfNeeded(engine, n, false)
				elem.FieldByName(code).Set(n)
			}
		}
		defaultInterface, is := value.Interface().(DefaultValuesInterface)
		if is {
			defaultInterface.SetDefaults()
		}
	}
	return orm
}
