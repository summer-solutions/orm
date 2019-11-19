package orm

import (
	"reflect"
)

type ReferenceMany struct {
	Ids        []uint64
	references []interface{}
	t          reflect.Type
}

func (r *ReferenceMany) Len() int {
	if r.Ids == nil {
		return 0
	}
	return len(r.Ids)
}

func (r *ReferenceMany) Has(id uint64) bool {
	if r.Ids == nil {
		return false
	}
	for _, val := range r.Ids {
		if id == val {
			return true
		}
	}
	return false
}

func (r *ReferenceMany) Add(ids ...uint64) {
	for _, id := range ids {
		if r.Has(id) {
			continue
		}
		if r.Ids == nil {
			r.Ids = make([]uint64, 0)
		}
		r.Ids = append(r.Ids, id)
	}
}

func (r *ReferenceMany) AddReference(entity ...interface{}) {
	for _, entity := range entity {
		id := reflect.ValueOf(entity).Elem().Field(1).Uint()
		if id > 0 {
			r.Add(id)
		} else {
			r.references = append(r.references, entity)
		}
	}
}

func (r *ReferenceMany) Remove(ids ...uint64) {
	if r.Ids == nil {
		return
	}
	for _, id := range ids {
		for i, val := range r.Ids {
			if id == val {
				r.Ids = append(r.Ids[:i], r.Ids[i+1:]...)
				return
			}
		}
	}
}

func (r *ReferenceMany) Clear() {
	r.Ids = nil
}

func (r *ReferenceMany) Load(entities interface{}) error {
	return GetByIds(r.Ids, entities)
}
