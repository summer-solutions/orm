package tests

import (
	"fmt"
	"github.com/summer-solutions/orm"
)

func PrepareTables(entities ...interface{}) (TableSchema *orm.TableSchema) {
	orm.RegisterMySqlPool("root:root@tcp(localhost:3310)/test")
	orm.RegisterRedis("localhost:6379", 15).FlushDB()
	orm.RegisterRedis("localhost:6379", 14, "default_queue").FlushDB()
	orm.SetRedisForQueue("default_queue")
	orm.RegisterLocalCache(1000)

	orm.RegisterEntity(entities...)
	orm.DisableContextCache()
	for _, entity := range entities {
		TableSchema = orm.GetTableSchema(entity)
		TableSchema.DropTable()
		TableSchema.UpdateSchema()
		localCache := TableSchema.GetLocalCache()
		if localCache != nil {
			localCache.Clear()
		}
	}
	return
}

type TestDatabaseLogger struct {
	Queries []string
}

func (l *TestDatabaseLogger) Log(mysqlCode string, query string, microseconds int64, args ...interface{}) {
	l.Queries = append(l.Queries, fmt.Sprintf("%s %v", query, args))
}

type TestCacheLogger struct {
	Requests []string
}

func (c *TestCacheLogger) Log(cacheType string, code string, key string, operation string, microseconds int64, misses int) {
	c.Requests = append(c.Requests, fmt.Sprintf("%s %s", operation, key))
}
