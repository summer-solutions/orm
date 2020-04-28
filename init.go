package orm

import (
	"fmt"
	"reflect"
)

func initEntityIfNeeded(engine *Engine, entity Entity) *ORM {
	return initIfNeeded(engine, entity)
}

func initIfNeeded(engine *Engine, entity Entity) *ORM {
	orm := entity.getORM()
	if orm.dBData == nil {
		value := reflect.ValueOf(entity)
		elem := value.Elem()
		t := elem.Type()
		tableSchema := getTableSchema(engine.registry, t)
		if tableSchema == nil {
			panic(fmt.Errorf("entity '%s' is registered", t.String()))
		}
		orm.engine = engine
		orm.tableSchema = tableSchema
		orm.dBData = make(map[string]interface{})
		orm.attributes = &entityAttributes{nil, false, false, value, elem, elem.Field(1)}
		defaultInterface, is := entity.(DefaultValuesInterface)
		if is {
			defaultInterface.SetDefaults()
		}
	}
	return orm
}
