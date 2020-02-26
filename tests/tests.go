package tests

import (
	"fmt"
	"github.com/summer-solutions/orm"
)

func PrepareTables(entities ...interface{}) (TableSchema *orm.TableSchema) {
	_ = orm.RegisterMySqlPool("root:root@tcp(localhost:3308)/test")
	_ = orm.RegisterRedis("localhost:6379", 15).FlushDB()
	_ = orm.RegisterRedis("localhost:6379", 14, "default_queue").FlushDB()
	orm.RegisterLazyQueue("default", "default_queue")
	orm.RegisterLocalCache(1000)

	orm.RegisterEntity(entities...)
	orm.DisableContextCache()
	for _, entity := range entities {
		TableSchema = orm.GetTableSchema(entity)
		err := TableSchema.DropTable()
		if err != nil {
			panic(err)
		}
		err = TableSchema.UpdateSchema()
		if err != nil {
			panic(err)
		}
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

func (l *TestDatabaseLogger) Logger() orm.DatabaseLogger {
	return func(mysqlCode string, query string, microseconds int64, args ...interface{}) {
		l.Queries = append(l.Queries, fmt.Sprintf("%s %v", query, args))
	}
}

type TestCacheLogger struct {
	Requests []string
}

func (c *TestCacheLogger) Logger() orm.CacheLogger {
	return func(cacheType string, code string, key string, operation string, microseconds int64, misses int) {
		c.Requests = append(c.Requests, fmt.Sprintf("%s %s", operation, key))
	}
}
