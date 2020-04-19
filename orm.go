package orm

import (
	"reflect"
)

type ORM struct {
	dBData      map[string]interface{}
	value       reflect.Value
	elem        reflect.Value
	tableSchema *TableSchema
}

func (orm ORM) Init(entity interface{}, engine *Engine) {
	initIfNeeded(engine, reflect.ValueOf(entity), true)
}

func (orm ORM) IsDirty() bool {
	is, _, _ := getDirtyBind(orm.elem)
	return is
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

func (orm ORM) Load(engine *Engine) error {
	if orm.Loaded() {
		return nil
	}
	return orm.Refresh(engine)
}

func (orm ORM) Refresh(engine *Engine) error {
	id := orm.elem.Field(1).Uint()
	if id == 0 {
		return nil
	}
	return engine.GetByID(id, orm.value.Interface())
}

func (orm ORM) ForceMarkToDelete() {
	orm.dBData["_delete"] = true
}
