package orm

import (
	"io/ioutil"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v2"
)

func TestYamlLoader(t *testing.T) {
	yamlFileData, err := ioutil.ReadFile("./config.yaml")
	assert.Nil(t, err)
	var parsedYaml map[string]interface{}
	err = yaml.Unmarshal(yamlFileData, &parsedYaml)
	assert.Nil(t, err)

	registry := InitByYaml(parsedYaml)
	assert.NotNil(t, registry)
	assert.Len(t, registry.redisStreamGroups, 2)
	assert.NotNil(t, registry.redisServers["another"])
	assert.Len(t, registry.redisStreamGroups["default"], 2)
	assert.Len(t, registry.redisStreamGroups["another"], 1)
	assert.Len(t, registry.redisStreamGroups["default"]["stream-1"], 2)
	assert.True(t, registry.redisStreamGroups["default"]["stream-1"]["test-group-1"])
	assert.True(t, registry.redisStreamGroups["default"]["stream-1"]["test-group-2"])
	assert.True(t, registry.redisStreamGroups["default"]["stream-2"]["test-group-1"])

	invalidYaml := make(map[string]interface{})
	invalidYaml["test"] = "invalid"
	assert.PanicsWithError(t, "orm yaml key orm is not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"mysql": []string{}}
	assert.PanicsWithError(t, "mysql uri '[]' is not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"elastic": []string{}}
	assert.PanicsWithError(t, "elastic uri '[]' is not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"clickhouse": []string{}}
	assert.PanicsWithError(t, "click house uri '[]' is not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"redis": "invalid"}
	assert.PanicsWithError(t, "redis uri 'invalid' is not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"redis": "invalid:invalid:invalid"}
	assert.PanicsWithError(t, "redis uri 'invalid:invalid:invalid' is not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"redis": []int{1}}
	assert.PanicsWithError(t, "redis uri '[1]' is not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": []int{1}}
	assert.PanicsWithError(t, "orm yaml key default is not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{}}
	assert.PanicsWithError(t, "rabbitMQ server definition 'default' not found", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": 1}}
	assert.PanicsWithError(t, "rabbitMQ server definition 'default' is not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "queues": 1}}
	assert.PanicsWithError(t, "rabbitMQ queues definition 'default' is not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "queues": []interface{}{1}}}
	assert.PanicsWithError(t, "orm yaml key default is not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "queues": []interface{}{map[interface{}]interface{}{"ss": 1}}}}
	assert.PanicsWithError(t, "rabbitMQ channel name 'default' not found", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "queues": []interface{}{map[interface{}]interface{}{"name": 1}}}}
	assert.PanicsWithError(t, "rabbitMQ channel name 'default' is not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "queues": []interface{}{map[interface{}]interface{}{"name": "test", "router": 1}}}}
	assert.PanicsWithError(t, "rabbitMQ router name 'default' is not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "queues": []interface{}{map[interface{}]interface{}{"name": "test", "router": "test", "router_keys": 1}}}}
	assert.PanicsWithError(t, "rabbitMQ router keys 'default' is not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "queues": []interface{}{map[interface{}]interface{}{"name": "test", "router": "test", "router_keys": []interface{}{1}}}}}
	assert.PanicsWithError(t, "rabbitMQ router key 'default' is not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "routers": 1}}
	assert.PanicsWithError(t, "rabbitMQ routers definition `default` is not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "routers": []interface{}{1}}}
	assert.PanicsWithError(t, "orm yaml key default is not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "routers": []interface{}{map[interface{}]interface{}{"a": 1}}}}
	assert.PanicsWithError(t, "rabbitMQ router name 'default' not found", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "routers": []interface{}{map[interface{}]interface{}{"name": 1}}}}
	assert.PanicsWithError(t, "rabbitMQ router name 'default' is not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "routers": []interface{}{map[interface{}]interface{}{"name": "test"}}}}
	assert.PanicsWithError(t, "rabbitMQ router type 'default' not found", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "routers": []interface{}{map[interface{}]interface{}{"name": "test", "type": 1}}}}
	assert.PanicsWithError(t, "rabbitMQ router type 'default' is not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"local_cache": "test"}
	assert.PanicsWithError(t, "orm value for default: test is not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"locker": 1}
	assert.PanicsWithError(t, "orm value for default: 1 is not valid", func() {
		registry = InitByYaml(invalidYaml)
	})
}
