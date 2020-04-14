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
	engine := orm.NewEngine(config)
	locker, has := engine.GetLocker()
	assert.True(t, has)
	lock, has, err := locker.Obtain("test", 10*time.Second)
	assert.Nil(t, err)
	assert.True(t, has)
	_, err = lock.TTL()
	assert.Nil(t, err)
	err = lock.Release()
	assert.Nil(t, err)
	locker, has = engine.GetLocker("test")
	assert.False(t, has)
	assert.Nil(t, locker)
}
