package orm

import (
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

func TestLocker(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6379", 5)
	registry.RegisterLocker("default", "default")
	registry.RegisterRabbitMQServer("amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test")
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()
	locker := engine.GetLocker()

	testLogger := memory.New()
	engine.AddQueryLogger(testLogger, log.InfoLevel, LoggerSourceRedis)

	lock, has := locker.Obtain("test", 10*time.Second, 0*time.Second)
	assert.True(t, has)
	assert.NotNil(t, lock)

	lock2, has := locker.Obtain("test", 10*time.Second, 500*time.Millisecond)
	assert.False(t, has)
	assert.Nil(t, lock2)

	_ = lock.TTL()
	lock.Release()
	lock.Release()
}
