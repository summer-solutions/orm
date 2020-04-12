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

func (r *ReferenceOne) Load(engine *Engine, entity interface{}) (bool, error) {
	if !r.Has() {
		return false, nil
	}
	has, err := engine.TryByID(r.ID, entity)
	if err != nil {
		return has, err
	}
	r.Reference = entity
	return has, err
}
