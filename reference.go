package orm

import (
	"reflect"
)

type ReferenceOne struct {
	Id uint64
	t  reflect.Type
}

func (r *ReferenceOne) Has() bool {
	return r.Id != 0
}

func (r *ReferenceOne) Load(entity interface{}) bool {
	if !r.Has() {
		return false
	}
	return TryById(r.Id, entity)
}
