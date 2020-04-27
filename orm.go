package orm

import (
	"reflect"
)

type Entity interface {
	getORM() *ORM
}

type entityAttributes struct {
	onDuplicateKeyUpdate *Where
	loaded               bool
	delete               bool
}

type ORM struct {
	dBData      map[string]interface{}
	value       reflect.Value
	elem        reflect.Value
	tableSchema *tableSchema
	engine      *Engine
	attributes  *entityAttributes
}

func (orm ORM) getORM() *ORM {
	return &orm
}
