package orm

import (
	"fmt"
	"reflect"
)

func initEntityIfNeeded(engine *Engine, entity Entity) *ORM {
	if entity.getORM().tableSchema == nil {
		return initIfNeeded(engine, reflect.ValueOf(entity))
	}
	return initIfNeeded(engine, entity.getORM().attributes.value)
}

func initIfNeeded(engine *Engine, value reflect.Value) *ORM {
	elem := value.Elem()
	address := elem.Field(0).Addr()
	orm := address.Interface().(*ORM)
	if orm.dBData == nil {
		tableSchema := getTableSchema(engine.registry, elem.Type())
		if tableSchema == nil {
			panic(fmt.Errorf("entity '%s' is registered", value.Type().String()))
		}
		orm.engine = engine
		orm.tableSchema = tableSchema
		orm.dBData = make(map[string]interface{})
		orm.attributes = &entityAttributes{nil, false,false, value, elem}
		defaultInterface, is := value.Interface().(DefaultValuesInterface)
		if is {
			defaultInterface.SetDefaults()
		}
	}
	return orm
}
