package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"testing"
)

func TestConfig(t *testing.T) {
	registry := orm.Registry{}
	config, err := registry.CreateConfig()
	assert.Nil(t, err)
	engine := config.CreateEngine()
	assert.NotNil(t, engine)
}
