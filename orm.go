package orm

import (
	"reflect"
)

type ORM struct {
	dBData      map[string]interface{}
	value       reflect.Value
	elem        reflect.Value
	tableSchema *TableSchema
	engine      *Engine
}

func (orm ORM) GetTableSchema() *TableSchema {
	return orm.tableSchema
}

func (orm ORM) IsDirty() bool {
	is, _, _ := getDirtyBind(orm.elem)
	return is
}

func (orm ORM) Flush() error {
	return flush(orm.engine, false, orm.value)
}

func (orm ORM) FlushLazy() error {
	return flush(orm.engine, true, orm.value)
}

func (orm ORM) MarkToDelete() {
	if orm.tableSchema.hasFakeDelete {
		orm.elem.FieldByName("FakeDelete").SetBool(true)
		return
	}
	orm.dBData["_delete"] = true
}

func (orm ORM) Loaded() bool {
	_, has := orm.dBData["_loaded"]
	return has
}

func (orm ORM) Load(engine *Engine) (has bool, err error) {
	id := orm.elem.Field(1).Uint()
	if id == 0 {
		return false, nil
	}
	return engine.LoadByID(id, orm.value.Interface())
}

func (orm ORM) ForceMarkToDelete() {
	orm.dBData["_delete"] = true
}
