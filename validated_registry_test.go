package orm

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

type validatedRegistryEntity struct {
	ORM
	ID uint
}

type validatedRegistryNotRegisteredEntity struct {
	ORM
	ID uint
}

func TestValidatedRegistry(t *testing.T) {
	registry := &Registry{}
	registry.RegisterMySQLPool("root:root@tcp(localhost:3310)/test")
	registry.RegisterEnumMap("enum_map", map[string]string{"a": "a", "b": "b"}, "a")
	entity := &validatedRegistryEntity{}
	registry.RegisterEntity(entity)
	validated, err := registry.Validate()
	assert.NoError(t, err)
	source := validated.GetSourceRegistry()
	assert.NotNil(t, source)
	entities := validated.GetEntities()
	assert.Len(t, entities, 1)
	assert.Equal(t, reflect.TypeOf(validatedRegistryEntity{}), entities["orm.validatedRegistryEntity"])
	enums := validated.GetEnums()
	assert.Len(t, enums, 1)
	assert.Nil(t, validated.GetTableSchema("invalid"))

	enum := validated.GetEnum("enum_map")
	assert.Equal(t, []string{"a", "b"}, enum.GetFields())
	assert.Equal(t, "a", enum.GetDefault())
	assert.True(t, enum.Has("a"))
	assert.False(t, enum.Has("c"))
	assert.Len(t, enum.GetMapping(), 2)

	registry.RegisterEnumSlice("enum_map", []string{"a", "b"})
	validated, err = registry.Validate()
	assert.NoError(t, err)
	enum = validated.GetEnum("enum_map")
	assert.Equal(t, []string{"a", "b"}, enum.GetFields())
	assert.Equal(t, "a", enum.GetDefault())
	assert.True(t, enum.Has("a"))
	assert.False(t, enum.Has("c"))
	assert.Len(t, enum.GetMapping(), 2)

	assert.PanicsWithValue(t, EntityNotRegisteredError{Name: "orm.validatedRegistryNotRegisteredEntity"}, func() {
		validated.GetTableSchemaForEntity(&validatedRegistryNotRegisteredEntity{})
	})
}
