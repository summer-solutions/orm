package tests

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"gopkg.in/yaml.v2"
)

func TestYamlLoader(t *testing.T) {
	yamlData, err := ioutil.ReadFile("config.yaml")
	assert.Nil(t, err)
	data := make(map[string]interface{})
	err = yaml.Unmarshal(yamlData, &data)
	assert.Nil(t, err)
	registry, err := orm.InitByYaml(data)
	assert.Nil(t, err)
	assert.NotNil(t, registry)
	config, err := registry.CreateConfig()
	assert.Nil(t, err)
	assert.NotNil(t, config)

	codes := config.GetLazyQueueCodes()
	assert.Equal(t, []string{"default"}, codes)
	codes = config.GetDirtyQueueCodes()
	assert.Equal(t, []string{"default"}, codes)

	schema, has := config.GetTableSchema("test")
	assert.Nil(t, schema)
	assert.False(t, has)

	invalidMap := make(map[string]interface{})
	invalidMap["ss"] = "vv"
	registry, err = orm.InitByYaml(invalidMap)
	assert.Nil(t, registry)
	assert.EqualError(t, err, "invalid orm section in config")

	invalidMySQL := make(map[string]interface{})
	invalidMySQL["default"] = map[interface{}]interface{}{"mysql": 12}
	registry, err = orm.InitByYaml(invalidMySQL)
	assert.Nil(t, registry)
	assert.EqualError(t, err, "invalid mysql uri: 12")

	invalidRedis := make(map[string]interface{})
	invalidRedis["default"] = map[interface{}]interface{}{"redis": 12}
	registry, err = orm.InitByYaml(invalidRedis)
	assert.Nil(t, registry)
	assert.EqualError(t, err, "invalid redis uri: 12")

	invalidRedis = make(map[string]interface{})
	invalidRedis["default"] = map[interface{}]interface{}{"redis": "test"}
	registry, err = orm.InitByYaml(invalidRedis)
	assert.Nil(t, registry)
	assert.EqualError(t, err, "invalid redis uri: test")

	invalidRedis = make(map[string]interface{})
	invalidRedis["default"] = map[interface{}]interface{}{"redis": "test:ss:dd"}
	registry, err = orm.InitByYaml(invalidRedis)
	assert.Nil(t, registry)
	assert.EqualError(t, err, "invalid redis DB id: test:ss:dd")

	invalidLazyQueue := make(map[string]interface{})
	invalidLazyQueue["default"] = map[interface{}]interface{}{"lazyQueue": 12}
	registry, err = orm.InitByYaml(invalidLazyQueue)
	assert.Nil(t, registry)
	assert.EqualError(t, err, "invalid orm value for default: 12")

	invalidLocker := make(map[string]interface{})
	invalidLocker["default"] = map[interface{}]interface{}{"locker": 12}
	registry, err = orm.InitByYaml(invalidLocker)
	assert.Nil(t, registry)
	assert.EqualError(t, err, "invalid orm value for default: 12")

	invalidDirtyQueue := make(map[string]interface{})
	invalidDirtyQueue["default"] = map[interface{}]interface{}{"dirtyQueue": 12}
	registry, err = orm.InitByYaml(invalidDirtyQueue)
	assert.Nil(t, registry)
	assert.EqualError(t, err, "invalid orm value for default: 12")

	invalidLocalCache := make(map[string]interface{})
	invalidLocalCache["default"] = map[interface{}]interface{}{"localCache": "test"}
	registry, err = orm.InitByYaml(invalidLocalCache)
	assert.Nil(t, registry)
	assert.EqualError(t, err, "invalid orm value for default: test")
}
