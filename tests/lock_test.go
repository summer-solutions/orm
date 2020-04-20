package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

func TestLock(t *testing.T) {
	registry := &orm.Registry{}
	registry.RegisterRedis("localhost:6379", 5)
	registry.RegisterLocker("default", "default")
	config, err := registry.CreateConfig()
	assert.Nil(t, err)
	engine := config.CreateEngine()
	locker, has := engine.GetLocker()
	assert.True(t, has)

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
	err = lock.Release()
	assert.Nil(t, err)
	err = lock.Release()
	assert.Nil(t, err)
	locker, has = engine.GetLocker("test")
	assert.False(t, has)
	assert.Nil(t, locker)

	lock, has, err = locker.Obtain("test", 0*time.Second, 10*time.Second)
	assert.Nil(t, lock)
	assert.False(t, has)
	assert.NotNil(t, err)
	assert.EqualError(t, err, "ttl must be greater than zero")
}
