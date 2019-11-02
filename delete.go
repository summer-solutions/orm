package orm

import (
	_ "github.com/go-sql-driver/mysql"
	"reflect"
)

func MarkToDelete(entities ...interface{}) {
	for _, entity := range entities {
		value := reflect.Indirect(reflect.ValueOf(entity))
		orm := value.Field(0).Interface().(ORM)
		orm.DBData["_delete"] = true
	}
}
