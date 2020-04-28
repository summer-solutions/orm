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
	value                reflect.Value
	elem                 reflect.Value
	idElem               reflect.Value
}

func (a *entityAttributes) getID() uint64 {
	return a.idElem.Uint()
}


type ORM struct {
	dBData      map[string]interface{}
	tableSchema *tableSchema
	engine      *Engine
	attributes  *entityAttributes
}

func (orm ORM) getORM() *ORM {
	return &orm
}
