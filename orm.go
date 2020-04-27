package orm

import (
	"reflect"
)

type Entity interface {
	Loaded() bool
	getDBData() map[string]interface{}
	getTableSchema() *tableSchema
	getValue() reflect.Value
	getElem() reflect.Value
	getORM() ORM
}

type ORM struct {
	dBData               map[string]interface{}
	value                reflect.Value
	elem                 reflect.Value
	tableSchema          *tableSchema
	engine               *Engine
	loaded               bool
	onDuplicateKeyUpdate *Where
}

func (orm ORM) Loaded() bool {
	return orm.loaded
}

func (orm *ORM) SetOnDuplicateKeyUpdate(update *Where) {
	orm.onDuplicateKeyUpdate = update
}

func (orm ORM) getDBData() map[string]interface{} {
	return orm.dBData
}

func (orm ORM) getTableSchema() *tableSchema {
	return orm.tableSchema
}

func (orm ORM) getValue() reflect.Value {
	return orm.value
}

func (orm ORM) getElem() reflect.Value {
	return orm.elem
}

func (orm ORM) getORM() ORM {
	return orm
}
