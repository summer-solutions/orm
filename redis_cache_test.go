package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/apex/log"

	"github.com/apex/log/handlers/memory"
)

func TestBasicRedis(t *testing.T) {
	r, engine := prepareRedis(t)
	testLogger := memory.New()
	engine.AddQueryLogger(testLogger, log.InfoLevel, QueryLoggerSourceRedis)
	mockClient := &mockRedisClient{client: r.client}
	r.client = mockClient

	val := r.GetSet("test", 1, func() interface{} {
		return "hello"
	})
	assert.Equal(t, "hello", val)
	assert.Len(t, testLogger.Entries, 2)
	assert.Equal(t, "[ORM][REDIS][GET]", testLogger.Entries[0].Message)
	assert.Equal(t, "[ORM][REDIS][SET]", testLogger.Entries[1].Message)
	val = r.GetSet("test", 1, func() interface{} {
		return "hello"
	})
	assert.Equal(t, "hello", val)
	assert.Len(t, testLogger.Entries, 3)
	assert.Equal(t, "[ORM][REDIS][GET]", testLogger.Entries[2].Message)
}

func TestList(t *testing.T) {
	r, engine := prepareRedis(t)
	testLogger := memory.New()
	engine.AddQueryLogger(testLogger, log.InfoLevel, QueryLoggerSourceRedis)
	mockClient := &mockRedisClient{client: r.client}
	r.client = mockClient
}

func TestHash(t *testing.T) {
	r, engine := prepareRedis(t)
	testLogger := memory.New()
	engine.AddQueryLogger(testLogger, log.InfoLevel, QueryLoggerSourceRedis)
	mockClient := &mockRedisClient{client: r.client}
	r.client = mockClient
}

func TestSet(t *testing.T) {
	r, _ := prepareRedis(t)
	mockClient := &mockRedisClient{client: r.client}
	r.client = mockClient
}

func TestSortedSet(t *testing.T) {
	r, engine := prepareRedis(t)
	testLogger := memory.New()
	engine.AddQueryLogger(testLogger, log.InfoLevel, QueryLoggerSourceRedis)
	mockClient := &mockRedisClient{client: r.client}
	r.client = mockClient
}

func prepareRedis(t *testing.T) (*RedisCache, *Engine) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6379", 15)
	registry.RegisterRabbitMQServer("amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test")
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()
	r := engine.GetRedis()
	mockClient := &mockRedisClient{client: r.client}
	r.client = mockClient
	r.FlushDB()
	return r, engine
}
