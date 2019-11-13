package orm

import (
	"reflect"
)

type ReferenceOne struct {
	Id     uint64
	t      reflect.Type
	loaded bool
	entity interface{}
}

func (r *ReferenceOne) Has() bool {
	return r.Id != 0
}

func (r *ReferenceOne) Get() interface{} {
	if !r.Has() {
		return nil
	}
	if !r.loaded {
		r.entity = reflect.New(r.t).Elem()
		has := TryById(r.Id, &r.entity)
		r.loaded = true
		if !has {
			r.entity = nil
		}
	}
	return r.entity
}
