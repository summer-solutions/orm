package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/summer-solutions/orm"
)

type TestEntityOnDuplicated struct {
	orm.ORM `orm:"redisCache"`
	ID      uint16
	Name    string `orm:"unique=NameIndex"`
	Counter int
}

func TestFlushOnDuplicated(t *testing.T) {
	registry := &orm.Registry{}
	registry.RegisterEnum("tests.Color", Color)
	var entity TestEntityOnDuplicated
	engine := PrepareTables(t, registry, entity)

	entity = TestEntityOnDuplicated{Name: "test", Counter: 7}
	err := engine.TrackAndFlush(&entity)
	assert.Nil(t, err)

	entity = TestEntityOnDuplicated{Name: "test"}
	err = engine.TrackAndFlush(&entity)
	assert.EqualError(t, err, "Duplicate entry 'test' for key 'NameIndex'")
	engine.ClearTrackedEntities()

	entity.Name = "test2"
	engine.SetOnDuplicateKeyUpdate(orm.NewWhere("`Counter` = `Counter` + ?", 1), &entity)
	err = engine.TrackAndFlush(&entity)
	assert.Nil(t, err)
	assert.Equal(t, uint16(3), entity.ID)

	entity = TestEntityOnDuplicated{Name: "test2"}
	engine.SetOnDuplicateKeyUpdate(orm.NewWhere("`Counter` = `Counter` + ?", 1), &entity)
	err = engine.TrackAndFlush(&entity)
	assert.Nil(t, err)
	assert.Equal(t, uint16(3), entity.ID)

	entity = TestEntityOnDuplicated{Name: "test"}
	engine.SetOnDuplicateKeyUpdate(orm.NewWhere("`Counter` = `Counter` + ?", 0), &entity)
	err = engine.TrackAndFlush(&entity)
	assert.Nil(t, err)
	assert.Equal(t, uint16(1), entity.ID)
	assert.Equal(t, 7, entity.Counter)

	entity = TestEntityOnDuplicated{Name: "test"}
	engine.SetOnDuplicateKeyUpdate(orm.NewWhere(""), &entity)
	err = engine.TrackAndFlush(&entity)
	assert.Nil(t, err)
	assert.Equal(t, uint16(1), entity.ID)
	assert.Equal(t, 7, entity.Counter)
}
