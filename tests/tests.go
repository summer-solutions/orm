package tests

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

func PrepareTables(t *testing.T, registry *orm.Registry, entities ...interface{}) *orm.Engine {
	registry.RegisterMySQLPool("root:root@tcp(localhost:3308)/test")
	registry.RegisterRedis("localhost:6379", 15)
	registry.RegisterRedis("localhost:6379", 14, "default_queue")
	registry.RegisterLazyQueue("default", "default_queue")
	registry.RegisterLocalCache(1000)

	registry.RegisterEntity(entities...)
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)

	engine := validatedRegistry.CreateEngine()
	assert.Equal(t, engine.GetRegistry(), validatedRegistry)
	redisCache := engine.GetRedis()
	err = redisCache.FlushDB()
	assert.Nil(t, err)
	redisCache = engine.GetRedis("default_queue")
	err = redisCache.FlushDB()
	assert.Nil(t, err)

	alters, err := engine.GetAlters()
	assert.Nil(t, err)
	for _, alter := range alters {
		pool := engine.GetMysql(alter.Pool)
		_, err := pool.Exec(alter.SQL)
		assert.Nil(t, err)
	}

	for _, entity := range entities {
		eType := reflect.TypeOf(entity)
		if eType.Kind() == reflect.Ptr {
			eType = eType.Elem()
		}
		tableSchema := validatedRegistry.GetTableSchema(eType.String())
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

func (l *TestDatabaseLogger) Log(_ string, query string, _ int64, args ...interface{}) {
	l.Queries = append(l.Queries, fmt.Sprintf("%s %v", query, args))
}

type TestCacheLogger struct {
	Requests []string
}

func (c *TestCacheLogger) Log(_ string, _ string, key string, operation string, _ int64, _ int) {
	c.Requests = append(c.Requests, fmt.Sprintf("%s %s", operation, key))
}
