package orm

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/stretchr/testify/assert"
)

type testEntityValidatedRegistry struct {
	ORM
	ID uint
}
type testEntityValidatedRegistryUnregistered struct {
	ORM
	ID uint
}

func TestValidatedRegistry(t *testing.T) {
	registry := &Registry{}
	registry.RegisterEntity(&testEntityValidatedRegistry{})
	registry.RegisterMySQLPool("root:root@tcp(localhost:3310)/test")
	registry.RegisterRabbitMQServer("amqp://rabbitmq_user:rabbitmq_password@localhost:5677/test")
	vr, err := registry.Validate()
	assert.Nil(t, err)

	require.PanicsWithError(t, "entity 'orm.testEntityValidatedRegistryUnregistered' is not registered", func() {
		vr.GetTableSchemaForEntity(&testEntityValidatedRegistryUnregistered{})
	})
	schema := vr.GetTableSchemaForEntity(&testEntityValidatedRegistry{})
	assert.NotNil(t, schema)
}
