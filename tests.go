package orm

import (
	"database/sql"
	"reflect"
	"testing"
	"time"

	"github.com/go-redis/redis/v7"

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

func (db *testSQLDB) Begin() error {
	return nil
}

func (db *testSQLDB) Commit() error {
	return nil
}

func (db *testSQLDB) Rollback() (bool, error) {
	return false, nil
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
	registry.RegisterLazyQueue(&RedisQueueSenderReceiver{PoolName: "default_queue"})
	registry.RegisterLogQueue("log", &RedisQueueSenderReceiver{PoolName: "default_log"})

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

type mockRedisClient struct {
	client      redisClient
	GetMock     func(key string) (string, error)
	LRangeMock  func(key string, start, stop int64) ([]string, error)
	HMGetMock   func(key string, fields ...string) ([]interface{}, error)
	HGetAllMock func(key string) (map[string]string, error)
}

func (c *mockRedisClient) Get(key string) (string, error) {
	if c.GetMock != nil {
		return c.GetMock(key)
	}
	return c.client.Get(key)
}

func (c *mockRedisClient) LRange(key string, start, stop int64) ([]string, error) {
	if c.LRangeMock != nil {
		return c.LRangeMock(key, start, stop)
	}
	return c.client.LRange(key, start, stop)
}

func (c *mockRedisClient) HMGet(key string, fields ...string) ([]interface{}, error) {
	if c.HMGetMock != nil {
		return c.HMGetMock(key, fields...)
	}
	return c.client.HMGet(key, fields...)
}

func (c *mockRedisClient) HGetAll(key string) (map[string]string, error) {
	if c.HGetAllMock != nil {
		return c.HGetAllMock(key)
	}
	return c.client.HGetAll(key)
}

func (c *mockRedisClient) LPush(key string, values ...interface{}) (int64, error) {
	return c.client.LPush(key, values...)
}

func (c *mockRedisClient) RPush(key string, values ...interface{}) (int64, error) {
	return c.client.RPush(key, values...)
}

func (c *mockRedisClient) RPop(key string) (string, error) {
	return c.client.RPop(key)
}

func (c *mockRedisClient) LSet(key string, index int64, value interface{}) (string, error) {
	return c.client.LSet(key, index, value)
}

func (c *mockRedisClient) LRem(key string, count int64, value interface{}) (int64, error) {
	return c.client.LRem(key, count, value)
}

func (c *mockRedisClient) LTrim(key string, start, stop int64) (string, error) {
	return c.client.LTrim(key, start, stop)
}

func (c *mockRedisClient) ZCard(key string) (int64, error) {
	return c.client.ZCard(key)
}

func (c *mockRedisClient) SCard(key string) (int64, error) {
	return c.client.SCard(key)
}

func (c *mockRedisClient) ZCount(key string, min, max string) (int64, error) {
	return c.client.ZCount(key, min, max)
}

func (c *mockRedisClient) SPop(key string) (string, error) {
	return c.client.SPop(key)
}

func (c *mockRedisClient) SPopN(key string, max int64) ([]string, error) {
	return c.client.SPopN(key, max)
}

func (c *mockRedisClient) LLen(key string) (int64, error) {
	return c.client.LLen(key)
}

func (c *mockRedisClient) ZAdd(key string, members ...*redis.Z) (int64, error) {
	return c.client.ZAdd(key, members...)
}

func (c *mockRedisClient) SAdd(key string, members ...interface{}) (int64, error) {
	return c.client.SAdd(key, members...)
}

func (c *mockRedisClient) HMSet(key string, fields map[string]interface{}) (bool, error) {
	return c.client.HMSet(key, fields)
}

func (c *mockRedisClient) HSet(key string, field string, value interface{}) (int64, error) {
	return c.client.HSet(key, field, value)
}

func (c *mockRedisClient) MGet(keys ...string) ([]interface{}, error) {
	return c.client.MGet(keys...)
}

func (c *mockRedisClient) Set(key string, value interface{}, expiration time.Duration) error {
	return c.client.Set(key, value, expiration)
}

func (c *mockRedisClient) MSet(pairs ...interface{}) error {
	return c.client.MSet(pairs...)
}

func (c *mockRedisClient) Del(keys ...string) error {
	return c.client.Del(keys...)
}

func (c *mockRedisClient) FlushDB() error {
	return c.client.FlushDB()
}
