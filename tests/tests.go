package tests

import (
	"fmt"
	"github.com/summer-solutions/orm"
	"reflect"
)

func PrepareTables(entities ...interface{}) (TableSchema *orm.TableSchema) {
	orm.RegisterMySqlPool("default", "root:root@tcp(localhost:3310)/test")
	orm.RegisterRedis("default", "localhost:6379", 15).FlushDB()
	orm.RegisterEntity(entities...)
	orm.DisableContextCache()
	for _, entity := range entities {
		TableSchema = orm.GetTableSchema(reflect.TypeOf(entity).String())
		TableSchema.DropTable()
		TableSchema.UpdateSchema()
		localCache := TableSchema.GetLocalCacheContainer()
		if localCache != nil {
			localCache.Clear()
		}
	}
	return
}

type TestDatabaseLogger struct {
	Queries []string
}

func (l *TestDatabaseLogger) Log(mysqlCode string, query string, args ...interface{}) {
	l.Queries = append(l.Queries, fmt.Sprintf("%s %v", query, args))
}

type TestCacheLogger struct {
	Requests []string
}

func (c *TestCacheLogger) Log(cacheType string, code string, key string, operation string, misses int) {
	c.Requests = append(c.Requests, fmt.Sprintf("%s %s", operation, key))
}
