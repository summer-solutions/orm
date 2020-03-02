package orm

import (
	"fmt"
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
	where.query += " " + query
	where.parameters = append(where.parameters, parameters...)
}

func NewWhere(query string, parameters ...interface{}) *Where {
	finalParameters := make([]interface{}, 0, len(parameters))
	for key, value := range parameters {
		switch reflect.TypeOf(value).Kind().String() {
		case "slice", "array":
			val := reflect.ValueOf(value)
			length := val.Len()
			in := strings.Repeat(",?", length)
			in = strings.TrimLeft(in, ",")
			query = replaceNth(query, "IN ?", fmt.Sprintf("IN (%s)", in), key+1)
			for i := 0; i < length; i++ {
				finalParameters = append(finalParameters, val.Index(i).Interface())
			}
			continue
		}
		finalParameters = append(finalParameters, value)
	}
	return &Where{query, finalParameters}
}

func replaceNth(s, old, new string, n int) string {
	i := 0
	for m := 1; m <= n; m++ {
		x := strings.Index(s[i:], old)
		if x < 0 {
			break
		}
		i += x
		if m == n {
			return s[:i] + new + s[i+len(old):]
		}
		i += len(old)
	}
	return s
}
