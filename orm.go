package orm

import (
	"reflect"
)

type Entity interface {
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

func (orm ORM) getORM() ORM {
	return orm
}
