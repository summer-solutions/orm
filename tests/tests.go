package tests

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"testing"
)

func PrepareTables(t *testing.T, config *orm.Config, entities ...interface{}) *orm.Engine {
	err := config.RegisterMySqlPool("root:root@tcp(localhost:3308)/test")
	assert.Nil(t, err)
	config.RegisterRedis("localhost:6379", 15)
	config.RegisterRedis("localhost:6379", 14, "default_queue")
	config.RegisterLazyQueue("default", "default_queue")
	config.RegisterLocalCache(1000)

	config.RegisterEntity(entities...)

	engine := orm.NewEngine(config)
	redisCache, has := engine.GetRedis()
	assert.True(t, has)
	err = redisCache.FlushDB()
	assert.Nil(t, err)
	redisCache, has = engine.GetRedis("default_queue")
	assert.True(t, has)
	err = redisCache.FlushDB()
	assert.Nil(t, err)

	alters, err := engine.GetAlters()
	assert.Nil(t, err)
	for _, alter := range alters {
		pool, has := engine.GetMysql(alter.Pool)
		assert.True(t, has)
		_, err := pool.Exec(alter.Sql)
		assert.Nil(t, err)
	}

	for _, entity := range entities {
		tableSchema, err := config.GetTableSchema(entity)
		assert.Nil(t, err)
		err = tableSchema.TruncateTable(engine)
		assert.Nil(t, err)
		err = tableSchema.UpdateSchema(engine)
		assert.Nil(t, err)
		localCache, has := tableSchema.GetLocalCache(engine)
		if has {
			localCache.Clear()
		}
	}
	return engine
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
