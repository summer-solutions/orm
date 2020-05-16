package orm

import (
	"testing"
	"time"

	"github.com/juju/errors"

	"github.com/bsm/redislock"

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
	locker.AddLogger(testLogger)
	locker.SetLogLevel(log.InfoLevel)
	assert.Equal(t, log.InfoLevel, locker.log.Level)

	lock, has, err := locker.Obtain("test", 10*time.Second, 0*time.Second)
	assert.Nil(t, err)
	assert.True(t, has)
	assert.NotNil(t, lock)

	lock2, has, err := locker.Obtain("test", 10*time.Second, 500*time.Millisecond)
	assert.Nil(t, err)
	assert.False(t, has)
	assert.Nil(t, lock2)

	_, err = lock.TTL()
	assert.Nil(t, err)
	lock.Release()
	lock.Release()

	lock, has, err = locker.Obtain("test", 0*time.Second, 10*time.Second)
	assert.Nil(t, lock)
	assert.False(t, has)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "ttl must be greater than zero")

	mockClient := &mockLockerClient{client: locker.locker}
	locker.locker = mockClient
	mockClient.ObtainMock = func(key string, ttl time.Duration, opt *redislock.Options) (*redislock.Lock, error) {
		return nil, errors.Errorf("test error")
	}
	_, _, err = locker.Obtain("test", 10*time.Second, 10*time.Second)
	assert.EqualError(t, err, "test error")

	locker.EnableDebug()
	locker.SetLogLevel(log.DebugLevel)
}
