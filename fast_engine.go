package orm

import (
	"fmt"
	"reflect"
)

type FastEngine interface {
	LoadByID(id uint64, entity Entity, references ...string) (found bool, fastEntity FastEntity)
	LoadByIDs(ids []uint64, entity Entity, references ...string) (result []FastEntity, missing []uint64)
}

type FastEntity interface {
	GetID() uint64
	Get(field string) interface{}
	Fill(entity Entity)
	Is(entity Entity) bool
}

type fastEngine struct {
	engine *Engine
}

func (fe *fastEngine) LoadByID(id uint64, entity Entity, references ...string) (found bool, result FastEntity) {
	found, data, schema := loadByID(fe.engine, id, entity, false, true, references...)
	if !found {
		return false, nil
	}
	return found, &fastEntity{data: data, engine: fe.engine, schema: schema}
}

func (fe *fastEngine) LoadByIDs(ids []uint64, entity Entity, references ...string) (result []FastEntity, missing []uint64) {
	missing, _ = tryByIDs(fe.engine, ids, false, reflect.ValueOf(entity), references)
	return nil, missing
}

type fastEntity struct {
	data   []interface{}
	engine *Engine
	schema *tableSchema
}

func (e *fastEntity) GetID() uint64 {
	return e.data[0].(uint64)
}

func (e *fastEntity) Get(field string) interface{} {
	return e.data[e.get(field)]
}

func (e *fastEntity) Fill(entity Entity) {
	orm := initIfNeeded(e.engine, entity)
	fillStruct(e.engine, 0, e.data, e.schema.fields, orm.elem)
}

func (e *fastEntity) Is(entity Entity) bool {
	return e.engine.registry.GetTableSchemaForEntity(entity) == e.schema
}

func (e *fastEntity) get(field string) int {
	i, has := e.schema.columnMapping[field]
	if !has {
		panic(fmt.Errorf("unknown field %s", field))
	}
	return i
}
