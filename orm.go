package orm

import (
	"reflect"
)

type Entity interface {
	getORM() *ORM
}

type entityAttributes struct {
	onDuplicateKeyUpdate *Where
}

type ORM struct {
	dBData      map[string]interface{}
	value       reflect.Value
	elem        reflect.Value
	tableSchema *tableSchema
	engine      *Engine
	loaded      bool
	attributes  *entityAttributes
}

func (orm ORM) getORM() *ORM {
	return &orm
}
