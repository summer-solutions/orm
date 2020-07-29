package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestValidatedRegistry(t *testing.T) {
	registry := &Registry{}
	registry.RegisterEnumMap("enum_map", map[string]string{"a": "a", "b": "b"}, "a")
	validated, err := registry.Validate()
	assert.NoError(t, err)
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
}
