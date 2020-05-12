package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testEntityOnDuplicated struct {
	ORM     `orm:"redisCache"`
	ID      uint16
	Name    string `orm:"unique=NameIndex"`
	Counter int
}

func TestFlushOnDuplicated(t *testing.T) {
	registry := &Registry{}
	registry.RegisterEnumStruct("orm.colorEnum", colorEnum)
	var entity testEntityOnDuplicated
	engine := PrepareTables(t, registry, entity)
	defer engine.Defer()

	entity = testEntityOnDuplicated{Name: "test", Counter: 7}
	err := engine.TrackAndFlush(&entity)
	assert.Nil(t, err)

	entity = testEntityOnDuplicated{Name: "test"}
	err = engine.TrackAndFlush(&entity)
	assert.EqualError(t, err, "Duplicate entry 'test' for key 'NameIndex'")
	engine.ClearTrackedEntities()

	entity.Name = "test2"
	engine.SetOnDuplicateKeyUpdate(NewWhere("`Counter` = `Counter` + ?", 1), &entity)
	err = engine.TrackAndFlush(&entity)
	assert.Nil(t, err)
	assert.Equal(t, uint16(3), entity.ID)

	entity = testEntityOnDuplicated{Name: "test2"}
	engine.SetOnDuplicateKeyUpdate(NewWhere("`Counter` = `Counter` + ?", 1), &entity)
	err = engine.TrackAndFlush(&entity)
	assert.Nil(t, err)
	assert.Equal(t, uint16(3), entity.ID)

	entity = testEntityOnDuplicated{Name: "test"}
	engine.SetOnDuplicateKeyUpdate(NewWhere("`Counter` = `Counter` + ?", 0), &entity)
	err = engine.TrackAndFlush(&entity)
	assert.Nil(t, err)
	assert.Equal(t, uint16(1), entity.ID)
	assert.Equal(t, 7, entity.Counter)

	entity = testEntityOnDuplicated{Name: "test"}
	engine.SetOnDuplicateKeyUpdate(NewWhere(""), &entity)
	err = engine.TrackAndFlush(&entity)
	assert.Nil(t, err)
	assert.Equal(t, uint16(1), entity.ID)
	assert.Equal(t, 7, entity.Counter)
}
