package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRabbitMQ(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRabbitMQServer("amqp://rabbitmq_user:rabbitmq_password@localhost:5672/")
	registry.RegisterRabbitMQChannel("default", &RabbitMQChannelConfig{Name: "test_channel"})
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()

	engine.EnableDebug()

	r := engine.GetRabbitMQChannel("test_channel")
	assert.NotNil(t, r)
	defer r.Close()
	err = r.Publish([]byte("Hello"), false, false)
	assert.NoError(t, err)
}
