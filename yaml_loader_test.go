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
	registry, err := InitByYaml(data)
	assert.Nil(t, err)
	assert.NotNil(t, registry)
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	assert.NotNil(t, validatedRegistry)

	codes := validatedRegistry.GetDirtyQueues()
	assert.Len(t, codes, 1)

	schema := validatedRegistry.GetTableSchema("test")
	assert.Nil(t, schema)

	invalidMap := make(map[string]interface{})
	invalidMap["ss"] = "vv"
	registry, err = InitByYaml(invalidMap)
	assert.Nil(t, registry)
	assert.EqualError(t, err, "invalid orm section in config")

	invalidMySQL := make(map[string]interface{})
	invalidMySQL["default"] = map[interface{}]interface{}{"mysql": 12}
	registry, err = InitByYaml(invalidMySQL)
	assert.Nil(t, registry)
	assert.EqualError(t, err, "invalid mysql uri: 12")

	invalidRedis := make(map[string]interface{})
	invalidRedis["default"] = map[interface{}]interface{}{"redis": 12}
	registry, err = InitByYaml(invalidRedis)
	assert.Nil(t, registry)
	assert.EqualError(t, err, "invalid redis uri: 12")

	invalidRedis = make(map[string]interface{})
	invalidRedis["default"] = map[interface{}]interface{}{"redis": "test"}
	registry, err = InitByYaml(invalidRedis)
	assert.Nil(t, registry)
	assert.EqualError(t, err, "invalid redis uri: test")

	invalidRedis = make(map[string]interface{})
	invalidRedis["default"] = map[interface{}]interface{}{"redis": "test:ss:dd"}
	registry, err = InitByYaml(invalidRedis)
	assert.Nil(t, registry)
	assert.EqualError(t, err, "invalid redis DB id: test:ss:dd")

	invalidLocker := make(map[string]interface{})
	invalidLocker["default"] = map[interface{}]interface{}{"locker": 12}
	registry, err = InitByYaml(invalidLocker)
	assert.Nil(t, registry)
	assert.EqualError(t, err, "invalid orm value for default: 12")

	invalidDirtyQueue := make(map[string]interface{})
	invalidDirtyQueue["default"] = map[interface{}]interface{}{"dirtyQueueRedis": 12}
	registry, err = InitByYaml(invalidDirtyQueue)
	assert.Nil(t, registry)
	assert.EqualError(t, err, "invalid orm value for default: 12")

	invalidLocalCache := make(map[string]interface{})
	invalidLocalCache["default"] = map[interface{}]interface{}{"localCache": "test"}
	registry, err = InitByYaml(invalidLocalCache)
	assert.Nil(t, registry)
	assert.EqualError(t, err, "invalid orm value for default: test")
}
