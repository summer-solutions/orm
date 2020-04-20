package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

func TestConfig(t *testing.T) {
	registry := orm.Registry{}
	config, err := registry.CreateConfig()
	assert.Nil(t, err)
	engine := config.CreateEngine()

	var entity time.Time

	has, err := engine.LoadByID(1, &entity)
	assert.False(t, has)
	assert.EqualError(t, err, "entity 'time.Time' is not registered")
}
