package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

func TestConfig(t *testing.T) {
	registry := orm.Registry{}
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()
	assert.NotNil(t, engine)
}
