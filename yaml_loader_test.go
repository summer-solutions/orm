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
	assert.Len(t, registry.redisStreamGroups["default"], 2)
	assert.Len(t, registry.redisStreamGroups["another"], 1)
	assert.Len(t, registry.redisStreamGroups["default"]["channel-1"], 2)
	assert.True(t, registry.redisStreamGroups["default"]["channel-1"]["test-group-1"])
	assert.True(t, registry.redisStreamGroups["default"]["channel-1"]["test-group-2"])
	assert.True(t, registry.redisStreamGroups["default"]["channel-2"]["test-group-1"])

	invalidYaml := make(map[string]interface{})
	invalidYaml["test"] = "invalid"
	assert.PanicsWithError(t, "orm yaml key orm not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"mysql": []string{}}
	assert.PanicsWithError(t, "mysql uri '[]' not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"elastic": []string{}}
	assert.PanicsWithError(t, "elastic uri '[]' not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"clickhouse": []string{}}
	assert.PanicsWithError(t, "click house uri '[]' not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"redis": "invalid"}
	assert.PanicsWithError(t, "redis uri 'invalid' not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"redis": "invalid:invalid:invalid"}
	assert.PanicsWithError(t, "redis uri 'invalid:invalid:invalid' not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"redis": []interface{}{"invalid"}}
	assert.PanicsWithError(t, "redis uri 'invalid' not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"redis": []interface{}{"invalid:invalid:invalid"}}
	assert.PanicsWithError(t, "redis uri 'invalid:invalid:invalid' not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"redis": []int{1}}
	assert.PanicsWithError(t, "redis uri '[1]' not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": []int{1}}
	assert.PanicsWithError(t, "orm yaml key default not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{}}
	assert.PanicsWithError(t, "rabbitMQ server definition 'default' not found", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": 1}}
	assert.PanicsWithError(t, "rabbitMQ server definition 'default' not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "queues": 1}}
	assert.PanicsWithError(t, "rabbitMQ queues definition 'default' not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "queues": []interface{}{1}}}
	assert.PanicsWithError(t, "orm yaml key default not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "queues": []interface{}{map[interface{}]interface{}{"ss": 1}}}}
	assert.PanicsWithError(t, "rabbitMQ channel name 'default' not found", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "queues": []interface{}{map[interface{}]interface{}{"name": 1}}}}
	assert.PanicsWithError(t, "rabbitMQ channel name 'default' not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "queues": []interface{}{map[interface{}]interface{}{"name": "test", "router": 1}}}}
	assert.PanicsWithError(t, "rabbitMQ router name 'default' not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "queues": []interface{}{map[interface{}]interface{}{"name": "test", "router": "test", "router_keys": 1}}}}
	assert.PanicsWithError(t, "rabbitMQ router keys 'default' not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "queues": []interface{}{map[interface{}]interface{}{"name": "test", "router": "test", "router_keys": []interface{}{1}}}}}
	assert.PanicsWithError(t, "rabbitMQ router key 'default' not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "routers": 1}}
	assert.PanicsWithError(t, "rabbitMQ routers definition `default` not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "routers": []interface{}{1}}}
	assert.PanicsWithError(t, "orm yaml key default not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "routers": []interface{}{map[interface{}]interface{}{"a": 1}}}}
	assert.PanicsWithError(t, "rabbitMQ router name 'default' not found", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "routers": []interface{}{map[interface{}]interface{}{"name": 1}}}}
	assert.PanicsWithError(t, "rabbitMQ router name 'default' not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "routers": []interface{}{map[interface{}]interface{}{"name": "test"}}}}
	assert.PanicsWithError(t, "rabbitMQ router type 'default' not found", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"rabbitmq": map[interface{}]interface{}{"server": "amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test", "routers": []interface{}{map[interface{}]interface{}{"name": "test", "type": 1}}}}
	assert.PanicsWithError(t, "rabbitMQ router type 'default' not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"local_cache": "test"}
	assert.PanicsWithError(t, "orm value for default: test not valid", func() {
		registry = InitByYaml(invalidYaml)
	})

	invalidYaml = make(map[string]interface{})
	invalidYaml["default"] = map[string]interface{}{"locker": 1}
	assert.PanicsWithError(t, "orm value for default: 1 not valid", func() {
		registry = InitByYaml(invalidYaml)
	})
}
