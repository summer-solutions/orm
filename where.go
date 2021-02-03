package orm

import (
	"reflect"
	"strings"
)

type Where struct {
	query      string
	parameters []interface{}
}

func (where *Where) String() string {
	return where.query
}

func (where *Where) GetParameters() []interface{} {
	return where.parameters
}

func (where *Where) Append(query string, parameters ...interface{}) {
	newWhere := NewWhere(query, parameters...)
	where.query += " " + newWhere.query
	where.parameters = append(where.parameters, newWhere.parameters...)
}

func NewWhere(query string, parameters ...interface{}) *Where {
	finalParameters := make([]interface{}, 0, len(parameters))
	for _, value := range parameters {
		switch reflect.TypeOf(value).Kind().String() {
		case "slice", "array":
			val := reflect.ValueOf(value)
			length := val.Len()
			in := strings.Repeat(",?", length)
			in = strings.TrimLeft(in, ",")
			query = strings.Replace(query, "IN ?", "IN ("+in+")", 1)
			for i := 0; i < length; i++ {
				finalParameters = append(finalParameters, val.Index(i).Interface())
			}
			continue
		}
		finalParameters = append(finalParameters, value)
	}
	return &Where{query, finalParameters}
}
