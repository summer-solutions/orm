package orm

import (
	"reflect"
)

type ReferenceOne struct {
	ID        uint64
	Reference interface{}
	t         reflect.Type
}

func (r *ReferenceOne) Has() bool {
	return r.ID != 0
}

func (r *ReferenceOne) Load(engine *Engine, entity interface{}) error {
	if !r.Has() {
		return nil
	}
	err := engine.GetByID(r.ID, entity)
	r.Reference = entity
	return err
}
