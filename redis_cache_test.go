package orm

import (
	"testing"
	"time"

	"github.com/juju/errors"

	"github.com/stretchr/testify/assert"

	"github.com/apex/log"

	"github.com/apex/log/handlers/memory"

	"github.com/go-redis/redis/v7"
)

func TestRedisEnableDebug(t *testing.T) {
	r := prepareRedis(t)
	r.EnableDebug()
	assert.NotNil(t, r.log)
	assert.Equal(t, log.DebugLevel, r.log.Level)
}

func TestBasicRedis(t *testing.T) {
	r := prepareRedis(t)
	testLogger := memory.New()
	r.AddLogger(testLogger)
	r.SetLogLevel(log.InfoLevel)
	mockClient := &mockRedisClient{client: r.client}
	r.client = mockClient

	val, err := r.GetSet("test", 1, func() interface{} {
		return "hello"
	})
	assert.Nil(t, err)
	assert.Equal(t, "hello", val)
	assert.Len(t, testLogger.Entries, 2)
	assert.Equal(t, "[ORM][REDIS][GET]", testLogger.Entries[0].Message)
	assert.Equal(t, "[ORM][REDIS][SET]", testLogger.Entries[1].Message)
	val, err = r.GetSet("test", 1, func() interface{} {
		return "hello"
	})
	assert.Nil(t, err)
	assert.Equal(t, "hello", val)
	assert.Len(t, testLogger.Entries, 3)
	assert.Equal(t, "[ORM][REDIS][GET]", testLogger.Entries[2].Message)

	mockClient.GetMock = func(key string) (string, error) {
		return "", errors.Errorf("redis error")
	}
	val, err = r.GetSet("test", 1, func() interface{} {
		return "hello"
	})
	assert.EqualError(t, err, "redis error")
	assert.Nil(t, val)
	mockClient.GetMock = nil

	mockClient.SetMock = func(key string, value interface{}, expiration time.Duration) error {
		return errors.Errorf("redis error")
	}
	val, err = r.GetSet("test2", 1, func() interface{} {
		return "hello"
	})
	assert.EqualError(t, err, "redis error")
	assert.Nil(t, val)
	mockClient.SetMock = nil

	err = r.MSet("a", "a1", "b", "b1")
	assert.NoError(t, err)
	mockClient.MSetMock = func(pairs ...interface{}) error {
		return errors.Errorf("redis error")
	}
	err = r.MSet("a", "a1", "b", "b1")
	assert.EqualError(t, err, "redis error")
	mockClient.MSetMock = nil

	values, err := r.MGet("a", "b", "c")
	assert.NoError(t, err)
	assert.Len(t, values, 3)
	assert.Equal(t, "a1", values["a"])
	assert.Equal(t, "b1", values["b"])
	assert.Nil(t, values["c"])
	mockClient.MGetMock = func(keys ...string) ([]interface{}, error) {
		return nil, errors.Errorf("redis error")
	}
	_, err = r.MGet("a", "b", "c")
	assert.EqualError(t, err, "redis error")
}

func TestList(t *testing.T) {
	r := prepareRedis(t)
	testLogger := memory.New()
	r.AddLogger(testLogger)
	r.SetLogLevel(log.InfoLevel)
	mockClient := &mockRedisClient{client: r.client}
	r.client = mockClient

	total, err := r.LPush("key", "a", "b", "c")
	assert.Nil(t, err)
	assert.Equal(t, int64(3), total)
	mockClient.LPushMock = func(key string, values ...interface{}) (int64, error) {
		return 0, errors.Errorf("redis error")
	}
	total, err = r.LPush("key", "a", "b", "c")
	assert.EqualError(t, err, "redis error")
	assert.Equal(t, int64(0), total)
	mockClient.LPushMock = nil

	total, err = r.LLen("key")
	assert.Nil(t, err)
	assert.Equal(t, int64(3), total)
	mockClient.LLenMock = func(key string) (int64, error) {
		return 0, errors.Errorf("redis error")
	}
	total, err = r.LLen("key")
	assert.EqualError(t, err, "redis error")
	assert.Equal(t, int64(0), total)
	mockClient.LLenMock = nil

	total, err = r.RPush("key", "d")
	assert.Nil(t, err)
	assert.Equal(t, int64(4), total)
	mockClient.RPushMock = func(key string, values ...interface{}) (int64, error) {
		return 0, errors.Errorf("redis error")
	}
	total, err = r.RPush("key", "d")
	assert.EqualError(t, err, "redis error")
	assert.Equal(t, int64(0), total)
	mockClient.RPushMock = nil

	elements, err := r.LRange("key", 0, 1)
	assert.Nil(t, err)
	assert.Equal(t, []string{"c", "b"}, elements)
	mockClient.LRangeMock = func(key string, start, stop int64) ([]string, error) {
		return nil, errors.Errorf("redis error")
	}
	_, err = r.LRange("key", 0, 1)
	assert.EqualError(t, err, "redis error")
	mockClient.LRangeMock = nil

	err = r.LSet("key", 1, "f")
	assert.Nil(t, err)
	mockClient.LSetMock = func(key string, index int64, value interface{}) (string, error) {
		return "", errors.Errorf("redis error")
	}
	err = r.LSet("key", 0, 1)
	assert.EqualError(t, err, "redis error")
	mockClient.LSetMock = nil

	elements, err = r.LRange("key", 0, 1)
	assert.Nil(t, err)
	assert.Equal(t, []string{"c", "f"}, elements)
	mockClient.LRangeMock = func(key string, start, stop int64) ([]string, error) {
		return nil, errors.Errorf("redis error")
	}
	_, err = r.LRange("key", 0, 1)
	assert.EqualError(t, err, "redis error")
	mockClient.LRangeMock = nil

	err = r.LRem("key", 1, "c")
	assert.Nil(t, err)
	mockClient.LRemMock = func(key string, count int64, value interface{}) (int64, error) {
		return 0, errors.Errorf("redis error")
	}
	err = r.LRem("key", 1, "c")
	assert.EqualError(t, err, "redis error")
	mockClient.LRemMock = nil

	elements, err = r.LRange("key", 0, 5)
	assert.Nil(t, err)
	assert.Equal(t, []string{"f", "a", "d"}, elements)

	element, found, err := r.RPop("key")
	assert.Nil(t, err)
	assert.True(t, found)
	assert.Equal(t, "d", element)
	mockClient.RPopMock = func(key string) (string, error) {
		return "", errors.Errorf("redis error")
	}
	_, _, err = r.RPop("key")
	assert.EqualError(t, err, "redis error")
	mockClient.RPopMock = nil

	err = r.Del("key")
	assert.Nil(t, err)
	mockClient.DelMock = func(keys ...string) error {
		return errors.Errorf("redis error")
	}
	err = r.Del("key")
	assert.EqualError(t, err, "redis error")
	mockClient.DelMock = nil

	element, found, err = r.RPop("key")
	assert.Nil(t, err)
	assert.False(t, found)
	assert.Equal(t, "", element)

	total, err = r.LPush("key", "a", "b", "c")
	assert.Nil(t, err)
	assert.Equal(t, int64(3), total)
	mockClient.LPushMock = func(key string, values ...interface{}) (int64, error) {
		return 0, errors.Errorf("redis error")
	}
	_, err = r.LPush("key", "a", "b", "c")
	assert.EqualError(t, err, "redis error")
	mockClient.LPushMock = nil

	err = r.Ltrim("key", 1, 3)
	assert.Nil(t, err)
	mockClient.LTrimMock = func(key string, start, stop int64) (string, error) {
		return "", errors.Errorf("redis error")
	}
	err = r.Ltrim("key", 1, 3)
	assert.EqualError(t, err, "redis error")

	elements, err = r.LRange("key", 0, 10)
	assert.Nil(t, err)
	assert.Equal(t, []string{"b", "a"}, elements)
}

func TestHash(t *testing.T) {
	r := prepareRedis(t)
	testLogger := memory.New()
	r.AddLogger(testLogger)
	r.SetLogLevel(log.InfoLevel)
	mockClient := &mockRedisClient{client: r.client}
	r.client = mockClient

	err := r.HSet("key", "field_1", "a")
	assert.Nil(t, err)
	mockClient.HSetMock = func(key string, field string, value interface{}) (int64, error) {
		return 0, errors.Errorf("redis error")
	}
	err = r.HSet("key", "field_1", "a")
	assert.EqualError(t, err, "redis error")
	mockClient.HSetMock = nil

	fields, err := r.HMget("key", "field_1", "field_2")
	assert.Nil(t, err)
	assert.Equal(t, fields["field_1"], "a")
	assert.Nil(t, fields["field_2"])
	mockClient.HMGetMock = func(key string, fields ...string) ([]interface{}, error) {
		return nil, errors.Errorf("redis error")
	}
	_, err = r.HMget("key", "field_1", "field_2")
	assert.EqualError(t, err, "redis error")
	mockClient.HMGetMock = nil

	err = r.HMset("key", map[string]interface{}{"field_3": "c", "field_4": "d"})
	assert.Nil(t, err)
	mockClient.HMSetMock = func(key string, fields map[string]interface{}) (bool, error) {
		return false, errors.Errorf("redis error")
	}
	err = r.HMset("key", map[string]interface{}{"field_3": "c", "field_4": "d"})
	assert.EqualError(t, err, "redis error")
	mockClient.HMSetMock = nil

	fieldsAll, err := r.HGetAll("key")
	assert.Nil(t, err)
	assert.Len(t, fieldsAll, 3)
	assert.Contains(t, fieldsAll, "field_1")
	assert.Contains(t, fieldsAll, "field_3")
	assert.Contains(t, fieldsAll, "field_4")
	assert.Equal(t, "a", fieldsAll["field_1"], "a")
	assert.Equal(t, "c", fieldsAll["field_3"])
	assert.Equal(t, "d", fieldsAll["field_4"])
	mockClient.HGetAllMock = func(key string) (map[string]string, error) {
		return nil, errors.Errorf("redis error")
	}
	_, err = r.HGetAll("key")
	assert.EqualError(t, err, "redis error")
	mockClient.HGetAllMock = nil
}

func TestSet(t *testing.T) {
	r := prepareRedis(t)
	mockClient := &mockRedisClient{client: r.client}
	r.client = mockClient

	total, err := r.SAdd("key", "a", "b", "c")
	assert.Nil(t, err)
	assert.Equal(t, int64(3), total)
	mockClient.SAddMock = func(key string, members ...interface{}) (int64, error) {
		return 0, errors.Errorf("redis error")
	}
	_, err = r.SAdd("key", "a", "b", "c")
	assert.EqualError(t, err, "redis error")
	mockClient.SAddMock = nil

	total, err = r.SCard("key")
	assert.Nil(t, err)
	assert.Equal(t, int64(3), total)
	mockClient.SCardMock = func(key string) (int64, error) {
		return 0, errors.Errorf("redis error")
	}
	_, err = r.SCard("key")
	assert.EqualError(t, err, "redis error")
	mockClient.SCardMock = nil

	_, has, err := r.SPop("key")
	assert.Nil(t, err)
	assert.True(t, has)
	mockClient.SPopMock = func(key string) (string, error) {
		return "", errors.Errorf("redis error")
	}
	_, _, err = r.SPop("key")
	assert.EqualError(t, err, "redis error")
	mockClient.SPopMock = nil

	values, err := r.SPopN("key", 2)
	assert.Nil(t, err)
	assert.Len(t, values, 2)
	mockClient.SPopNMock = func(key string, max int64) ([]string, error) {
		return nil, errors.Errorf("redis error")
	}
	_, err = r.SPopN("key", 2)
	assert.EqualError(t, err, "redis error")
}

func TestSortedSet(t *testing.T) {
	r := prepareRedis(t)
	testLogger := memory.New()
	r.AddLogger(testLogger)
	r.SetLogLevel(log.InfoLevel)
	mockClient := &mockRedisClient{client: r.client}
	r.client = mockClient

	total, err := r.ZAdd("key", &redis.Z{Member: "a", Score: 100})
	assert.Nil(t, err)
	assert.Equal(t, int64(1), total)
	mockClient.ZAddMock = func(key string, members ...*redis.Z) (int64, error) {
		return 0, errors.Errorf("redis error")
	}
	_, err = r.ZAdd("key", &redis.Z{Member: "a", Score: 100})
	assert.EqualError(t, err, "redis error")
	mockClient.ZAddMock = nil

	total, err = r.ZAdd("key", &redis.Z{Member: "v", Score: 200})
	assert.Nil(t, err)
	assert.Equal(t, int64(1), total)

	total, err = r.ZCard("key")
	assert.Nil(t, err)
	assert.Equal(t, int64(2), total)
	mockClient.ZCardMock = func(key string) (int64, error) {
		return 0, errors.Errorf("redis error")
	}
	_, err = r.ZCard("key")
	assert.EqualError(t, err, "redis error")
	mockClient.ZCardMock = nil

	total, err = r.ZCount("key", "100", "150")
	assert.Nil(t, err)
	assert.Equal(t, int64(1), total)
	mockClient.ZCountMock = func(key string, min, max string) (int64, error) {
		return 0, errors.Errorf("redis error")
	}
	_, err = r.ZCount("key", "100", "150")
	assert.EqualError(t, err, "redis error")
}

func prepareRedis(t *testing.T) *RedisCache {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6379", 15)
	registry.RegisterRabbitMQServer("amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test")
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()
	r := engine.GetRedis()
	mockClient := &mockRedisClient{client: r.client}
	r.client = mockClient
	err = r.FlushDB()
	assert.Nil(t, err)
	mockClient.FlushDBMock = func() error {
		return errors.Errorf("redis error")
	}
	err = r.FlushDB()
	assert.EqualError(t, err, "redis error")
	return r
}
