package orm

import (
	"reflect"
)

type Entity interface {
	getORM() *ORM
	GetID() uint64
}

type entityAttributes struct {
	onDuplicateKeyUpdate *Where
	loaded               bool
	delete               bool
	value                reflect.Value
	elem                 reflect.Value
	idElem               reflect.Value
}

type ORM struct {
	dBData      map[string]interface{}
	tableSchema *tableSchema
	engine      *Engine
	attributes  *entityAttributes
}

func (orm *ORM) getORM() *ORM {
	return orm
}

func (orm *ORM) GetID() uint64 {
	if orm.attributes == nil {
		return 0
	}
	return orm.attributes.idElem.Uint()
}
