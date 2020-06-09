package orm

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

func TestYamlLoader(t *testing.T) {
	yamlData, err := ioutil.ReadFile("config.yaml")
	assert.Nil(t, err)
	data := make(map[string]interface{})
	err = yaml.Unmarshal(yamlData, &data)
	assert.Nil(t, err)
	registry := InitByYaml(data)
	assert.NotNil(t, registry)
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	assert.NotNil(t, validatedRegistry)

	codes := validatedRegistry.GetDirtyQueues()
	assert.Len(t, codes, 1)

	schema := validatedRegistry.GetTableSchema("test")
	assert.Nil(t, schema)
}
