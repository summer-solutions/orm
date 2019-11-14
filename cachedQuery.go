package orm

import "reflect"

type CachedQuery struct {
	query string
	t     reflect.Type
	max   uint64
}
