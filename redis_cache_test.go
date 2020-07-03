package orm

import (
	"testing"

	"github.com/go-redis/redis_rate/v8"

	"github.com/stretchr/testify/assert"
)

func TestRedis(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6380", 15)
	registry.RegisterRabbitMQServer("amqp://rabbitmq_user:rabbitmq_password@localhost:5677/test")
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	testRedis(t, validatedRegistry.CreateEngine())
}

func TestRedisRing(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedisRing([]string{"localhost:6380"}, 15)
	registry.RegisterRabbitMQServer("amqp://rabbitmq_user:rabbitmq_password@localhost:5677/test")
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	testRedis(t, validatedRegistry.CreateEngine())
}

func testRedis(t *testing.T, engine *Engine) {
	r := engine.GetRedis()
	r.FlushDB()

	assert.True(t, r.RateLimit("test", redis_rate.PerSecond(2)))
	assert.True(t, r.RateLimit("test", redis_rate.PerSecond(2)))
	assert.False(t, r.RateLimit("test", redis_rate.PerSecond(2)))
}
