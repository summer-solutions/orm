package orm

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testQueryFunc func(db sqlDB, counter int, query string, args ...interface{}) (SQLRows, error)
type testQueryRowFunc func(db sqlDB, counter int, query string, args ...interface{}) SQLRow

type testSQLDB struct {
	db           sqlDB
	counter      int
	QueryMock    testQueryFunc
	QueryRowMock testQueryRowFunc
}

func (db *testSQLDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.db.Exec(query, args...)
}

func (db *testSQLDB) QueryRow(query string, args ...interface{}) SQLRow {
	db.counter++
	if db.QueryRowMock != nil {
		return db.QueryRowMock(db.db, db.counter, query, args...)
	}
	return db.db.QueryRow(query, args...)
}

func (db *testSQLDB) Query(query string, args ...interface{}) (SQLRows, error) {
	db.counter++
	if db.QueryMock != nil {
		return db.QueryMock(db.db, db.counter, query, args...)
	}
	return db.db.Query(query, args...)
}

func mockDB(engine *Engine, poolCode string) *testSQLDB {
	db := engine.dbs[poolCode]
	testDB := &testSQLDB{db: db.db}
	db.db = testDB
	return testDB
}

func PrepareTables(t *testing.T, registry *Registry, entities ...interface{}) *Engine {
	registry.RegisterMySQLPool("root:root@tcp(localhost:3308)/test")
	registry.RegisterMySQLPool("root:root@tcp(localhost:3308)/test_log", "log")
	registry.RegisterRedis("localhost:6379", 15)
	registry.RegisterRedis("localhost:6379", 14, "default_queue")
	registry.RegisterRedis("localhost:6379", 13, "default_log")
	registry.RegisterLazyQueue("default", "default_queue")
	registry.RegisterLogQueue("log", &RedisLogQueueSender{PoolName: "default_log"})

	registry.RegisterLocalCache(1000)

	registry.RegisterEntity(entities...)
	config, err := registry.CreateConfig()
	assert.Nil(t, err)

	engine := config.CreateEngine()
	assert.Equal(t, engine.GetConfig(), config)
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
		tableSchema, _ := config.GetTableSchema(eType.String())
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
