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
	has, err := TryById(r.Id, entity)
	if err != nil {
		return has, err
	}
	r.Reference = entity
	return has, err
}
