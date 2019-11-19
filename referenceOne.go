package orm

import (
	"reflect"
)

type ReferenceOne struct {
	Id        uint64
	Reference interface{}
	t         reflect.Type
}

func (r *ReferenceOne) Has() bool {
	return r.Id != 0
}

func (r *ReferenceOne) Load(entity interface{}) (bool, error) {
	if !r.Has() {
		return false, nil
	}
	return TryById(r.Id, entity)
}
