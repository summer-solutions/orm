package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

func TestConfig(t *testing.T) {
	registry := orm.Registry{}
	config, err := registry.CreateConfig()
	assert.Nil(t, err)
	engine := config.CreateEngine()
	assert.NotNil(t, engine)
}
