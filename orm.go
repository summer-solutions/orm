package orm

import (
	"fmt"
	"reflect"
)

type Entity interface {
	GetTableSchema() TableSchema
	IsDirty() bool
	Flush() error
	FlushLazy() error
	MarkToDelete()
	Loaded() bool
	Load(engine *Engine) error
	ForceMarkToDelete()
	getDBData() map[string]interface{}
	getTableSchema() *tableSchema
	getValue() reflect.Value
	getElem() reflect.Value
}

type ORM struct {
	dBData      map[string]interface{}
	value       reflect.Value
	elem        reflect.Value
	tableSchema *tableSchema
	engine      *Engine
}

func (orm ORM) GetTableSchema() TableSchema {
	orm.checkIsRegistered()
	return orm.tableSchema
}

// Returns true if at least one filed needs to be saved in database
func (orm ORM) IsDirty() bool {
	orm.checkIsRegistered()
	is, _, _ := getDirtyBind(orm.elem)
	return is
}

// Save data in database (only if IsDirty() is true)
func (orm ORM) Flush() error {
	orm.checkIsRegistered()
	return flush(orm.engine, false, orm.value)
}

// Add changes to queue and all changes will be saved in database in separate thread
func (orm ORM) FlushLazy() error {
	orm.checkIsRegistered()
	return flush(orm.engine, true, orm.value)
}

// Mark entity to be deleted from database.
// Delete query is executed in Flush() method.
// If entity has FakeDelete column row will be kept in database
// with flag that is deleted.
func (orm ORM) MarkToDelete() {
	orm.checkIsRegistered()
	if orm.tableSchema.hasFakeDelete {
		orm.elem.FieldByName("FakeDelete").SetBool(true)
		return
	}
	orm.dBData["_delete"] = true
}

// Mark entity to be deleted from database even if it has FakeDelete column.
func (orm ORM) ForceMarkToDelete() {
	orm.checkIsRegistered()
	orm.dBData["_delete"] = true
}

// Returns false if entity is not loaded from DB.
// It can happen when you loaded entity and it's reference is not loaded from DB.
func (orm ORM) Loaded() bool {
	if orm.dBData == nil {
		return false
	}
	_, has := orm.dBData["_loaded"]
	return has
}

// You should execute it in references that are not loaded.
// This method do nothing if entity is already loaded or reference has ID = 0
// Example:
// engine.LoadById(1, &user)
// user.School.Load(engine)
func (orm ORM) Load(engine *Engine) error {
	if orm.Loaded() {
		return nil
	}
	id := orm.elem.Field(1).Uint()
	if id == 0 {
		return nil
	}
	_, err := engine.LoadByID(id, orm.value.Interface().(Entity))
	return err
}

func (orm ORM) checkIsRegistered() {
	if orm.tableSchema == nil {
		panic(fmt.Errorf("unregistered struct. run engine.RegisterEntity(entity) before entity.Flush()"))
	}
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
