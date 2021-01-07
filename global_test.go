package orm

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func PrepareTables(t *testing.T, registry *Registry, version int, entities ...Entity) *Engine {
	if version == 5 {
		registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test")
		registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test_log", "log")
	} else {
		registry.RegisterMySQLPool("root:root@tcp(localhost:3312)/test")
		registry.RegisterMySQLPool("root:root@tcp(localhost:3312)/test_log", "log")
	}
	registry.RegisterLocker("default", "default")
	registry.RegisterRedis("localhost:6381", 15)
	registry.RegisterRedis("localhost:6381", 14, "default_queue")
	registry.RegisterRabbitMQServer("amqp://rabbitmq_user:rabbitmq_password@localhost:5678/test")
	registry.RegisterLocalCache(1000)

	registry.RegisterEntity(entities...)
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)

	engine := validatedRegistry.CreateEngine()
	assert.Equal(t, engine.GetRegistry(), validatedRegistry)
	redisCache := engine.GetRedis()
	redisCache.FlushDB()
	redisCache = engine.GetRedis("default_queue")
	redisCache.FlushDB()

	alters := engine.GetAlters()
	for _, alter := range alters {
		pool := engine.GetMysql(alter.Pool)
		pool.Exec(alter.SQL)
	}

	altersElastic := engine.GetElasticIndexAlters()
	for _, alter := range altersElastic {
		pool := engine.GetElastic(alter.Pool)
		pool.CreateIndex(alter.Index)
	}

	engine.GetMysql().Exec("SET FOREIGN_KEY_CHECKS = 0")
	for _, entity := range entities {
		eType := reflect.TypeOf(entity)
		if eType.Kind() == reflect.Ptr {
			eType = eType.Elem()
		}
		tableSchema := validatedRegistry.GetTableSchema(eType.String())
		tableSchema.TruncateTable(engine)
		tableSchema.UpdateSchema(engine)
		localCache, has := tableSchema.GetLocalCache(engine)
		if has {
			localCache.Clear()
		}
	}
	engine.GetMysql().Exec("SET FOREIGN_KEY_CHECKS = 1")
	return engine
}

type mockDBClient struct {
	db           dbClient
	tx           dbClientTX
	ExecMock     func(query string, args ...interface{}) (sql.Result, error)
	QueryRowMock func(query string, args ...interface{}) *sql.Row
	QueryMock    func(query string, args ...interface{}) (*sql.Rows, error)
	BeginMock    func() (*sql.Tx, error)
	CommitMock   func() error
	RollbackMock func() error
}

func (m *mockDBClient) Exec(query string, args ...interface{}) (sql.Result, error) {
	if m.ExecMock != nil {
		return m.ExecMock(query, args...)
	}
	return m.db.Exec(query, args...)
}

func (m *mockDBClient) QueryRow(query string, args ...interface{}) *sql.Row {
	if m.QueryRowMock != nil {
		return m.QueryRowMock(query, args...)
	}
	return m.db.QueryRow(query, args...)
}

func (m *mockDBClient) Query(query string, args ...interface{}) (*sql.Rows, error) {
	if m.QueryMock != nil {
		return m.QueryMock(query, args...)
	}
	return m.db.Query(query, args...)
}

func (m *mockDBClient) Begin() (*sql.Tx, error) {
	if m.BeginMock != nil {
		return m.BeginMock()
	}
	return m.db.Begin()
}

func (m *mockDBClient) Rollback() error {
	if m.RollbackMock != nil {
		return m.RollbackMock()
	}
	return m.tx.Rollback()
}

func (m *mockDBClient) Commit() error {
	if m.CommitMock != nil {
		return m.CommitMock()
	}
	return m.tx.Commit()
}

//type mockRedisClient struct {
//	client      redisClient
//	GetMock     func(key string) (string, error)
//	LRangeMock  func(key string, start, stop int64) ([]string, error)
//	HMGetMock   func(key string, fields ...string) ([]interface{}, error)
//	HGetAllMock func(key string) (map[string]string, error)
//	LPushMock   func(key string, values ...interface{}) (int64, error)
//	LLenMock    func(key string) (int64, error)
//	RPushMock   func(key string, values ...interface{}) (int64, error)
//	RPopMock    func(key string) (string, error)
//	LSetMock    func(key string, index int64, value interface{}) (string, error)
//	LRemMock    func(key string, count int64, value interface{}) (int64, error)
//	LTrimMock   func(key string, start, stop int64) (string, error)
//	ZCardMock   func(key string) (int64, error)
//	SCardMock   func(key string) (int64, error)
//	ZCountMock  func(key string, min, max string) (int64, error)
//	SPopNMock   func(key string, max int64) ([]string, error)
//	SPopMock    func(key string) (string, error)
//	ZAddMock    func(key string, members ...*redis.Z) (int64, error)
//	SAddMock    func(key string, members ...interface{}) (int64, error)
//	HMSetMock   func(key string, fields map[string]interface{}) (bool, error)
//	HSetMock    func(key string, field string, value interface{}) (int64, error)
//	MGetMock    func(keys ...string) ([]interface{}, error)
//	SetMock     func(key string, value interface{}, expiration time.Duration) error
//	MSetMock    func(pairs ...interface{}) error
//	DelMock     func(keys ...string) error
//	FlushDBMock func() error
//}

//func (c *mockRedisClient) Get(key string) (string, error) {
//	if c.GetMock != nil {
//		return c.GetMock(key)
//	}
//	return c.client.Get(key)
//}
//
//func (c *mockRedisClient) LRange(key string, start, stop int64) ([]string, error) {
//	if c.LRangeMock != nil {
//		return c.LRangeMock(key, start, stop)
//	}
//	return c.client.LRange(key, start, stop)
//}
//
//func (c *mockRedisClient) HMGet(key string, fields ...string) ([]interface{}, error) {
//	if c.HMGetMock != nil {
//		return c.HMGetMock(key, fields...)
//	}
//	return c.client.HMGet(key, fields...)
//}
//
//func (c *mockRedisClient) HGetAll(key string) (map[string]string, error) {
//	if c.HGetAllMock != nil {
//		return c.HGetAllMock(key)
//	}
//	return c.client.HGetAll(key)
//}
//
//func (c *mockRedisClient) LPush(key string, values ...interface{}) (int64, error) {
//	if c.LPushMock != nil {
//		return c.LPushMock(key, values...)
//	}
//	return c.client.LPush(key, values...)
//}
//
//func (c *mockRedisClient) RPush(key string, values ...interface{}) (int64, error) {
//	if c.RPushMock != nil {
//		return c.RPushMock(key, values...)
//	}
//	return c.client.RPush(key, values...)
//}
//
//func (c *mockRedisClient) RPop(key string) (string, error) {
//	if c.RPopMock != nil {
//		return c.RPopMock(key)
//	}
//	return c.client.RPop(key)
//}
//
//func (c *mockRedisClient) LSet(key string, index int64, value interface{}) (string, error) {
//	if c.LSetMock != nil {
//		return c.LSetMock(key, index, value)
//	}
//	return c.client.LSet(key, index, value)
//}
//
//func (c *mockRedisClient) LRem(key string, count int64, value interface{}) (int64, error) {
//	if c.LRemMock != nil {
//		return c.LRemMock(key, count, value)
//	}
//	return c.client.LRem(key, count, value)
//}
//
//func (c *mockRedisClient) LTrim(key string, start, stop int64) (string, error) {
//	if c.LTrimMock != nil {
//		return c.LTrimMock(key, start, stop)
//	}
//	return c.client.LTrim(key, start, stop)
//}
//
//func (c *mockRedisClient) ZCard(key string) (int64, error) {
//	if c.ZCardMock != nil {
//		return c.ZCardMock(key)
//	}
//	return c.client.ZCard(key)
//}
//
//func (c *mockRedisClient) SCard(key string) (int64, error) {
//	if c.SCardMock != nil {
//		return c.SCardMock(key)
//	}
//	return c.client.SCard(key)
//}
//
//func (c *mockRedisClient) ZCount(key string, min, max string) (int64, error) {
//	if c.ZCountMock != nil {
//		return c.ZCountMock(key, min, max)
//	}
//	return c.client.ZCount(key, min, max)
//}
//
//func (c *mockRedisClient) SPop(key string) (string, error) {
//	if c.SPopMock != nil {
//		return c.SPopMock(key)
//	}
//	return c.client.SPop(key)
//}
//
//func (c *mockRedisClient) SPopN(key string, max int64) ([]string, error) {
//	if c.SPopNMock != nil {
//		return c.SPopNMock(key, max)
//	}
//	return c.client.SPopN(key, max)
//}
//
//func (c *mockRedisClient) LLen(key string) (int64, error) {
//	if c.LLenMock != nil {
//		return c.LLenMock(key)
//	}
//	return c.client.LLen(key)
//}
//
//func (c *mockRedisClient) ZAdd(key string, members ...*redis.Z) (int64, error) {
//	if c.ZAddMock != nil {
//		return c.ZAddMock(key, members...)
//	}
//	return c.client.ZAdd(key, members...)
//}
//
//func (c *mockRedisClient) SAdd(key string, members ...interface{}) (int64, error) {
//	if c.SAddMock != nil {
//		return c.SAddMock(key, members...)
//	}
//	return c.client.SAdd(key, members...)
//}
//
//func (c *mockRedisClient) HMSet(key string, fields map[string]interface{}) (bool, error) {
//	if c.HMSetMock != nil {
//		return c.HMSetMock(key, fields)
//	}
//	return c.client.HMSet(key, fields)
//}
//
//func (c *mockRedisClient) HSet(key string, field string, value interface{}) (int64, error) {
//	if c.HSetMock != nil {
//		return c.HSetMock(key, field, value)
//	}
//	return c.client.HSet(key, field, value)
//}
//
//func (c *mockRedisClient) MGet(keys ...string) ([]interface{}, error) {
//	if c.MGetMock != nil {
//		return c.MGetMock(keys...)
//	}
//	return c.client.MGet(keys...)
//}
//
//func (c *mockRedisClient) Set(key string, value interface{}, expiration time.Duration) error {
//	if c.SetMock != nil {
//		return c.SetMock(key, value, expiration)
//	}
//	return c.client.Set(key, value, expiration)
//}
//
//func (c *mockRedisClient) MSet(pairs ...interface{}) error {
//	if c.MSetMock != nil {
//		return c.MSetMock(pairs...)
//	}
//	return c.client.MSet(pairs...)
//}
//
//func (c *mockRedisClient) Del(keys ...string) error {
//	if c.DelMock != nil {
//		return c.DelMock(keys...)
//	}
//	return c.client.Del(keys...)
//}
//
//func (c *mockRedisClient) FlushDB() error {
//	if c.FlushDBMock != nil {
//		return c.FlushDBMock()
//	}
//	return c.client.FlushDB()
//}
