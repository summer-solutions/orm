package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type loadByIDEntity struct {
	ORM          `orm:"localCache;redisCache"`
	ID           uint
	Name         string `orm:"max=100"`
	ReferenceOne *loadByIDReference
}

type loadByIDRedisEntity struct {
	ORM `orm:"redisCache"`
	ID  uint
}

type loadByIDNoCacheEntity struct {
	ORM
	ID   uint
	Name string
}

type loadByIDReference struct {
	ORM          `orm:"localCache;redisCache"`
	ID           uint
	Name         string
	ReferenceTwo *loadByIDSubReference
}

type loadByIDSubReference struct {
	ORM  `orm:"localCache;redisCache"`
	ID   uint
	Name string
}

func TestLoadById(t *testing.T) {
	var entity *loadByIDEntity
	var entityRedis *loadByIDRedisEntity
	var entityNoCache *loadByIDNoCacheEntity
	var reference *loadByIDReference
	var subReference *loadByIDSubReference
	engine := PrepareTables(t, &Registry{}, entity, entityRedis, entityNoCache, reference, subReference)

	engine.TrackAndFlush(&loadByIDEntity{Name: "a", ReferenceOne: &loadByIDReference{Name: "r1", ReferenceTwo: &loadByIDSubReference{Name: "s1"}}},
		&loadByIDEntity{Name: "b", ReferenceOne: &loadByIDReference{Name: "r2", ReferenceTwo: &loadByIDSubReference{Name: "s2"}}},
		&loadByIDEntity{Name: "c"}, &loadByIDNoCacheEntity{Name: "a"})

	entity = &loadByIDEntity{}
	found := engine.LoadByID(1, entity, "ReferenceOne/ReferenceTwo")
	assert.True(t, found)
	assert.Equal(t, uint(1), entity.ID)
	assert.Equal(t, "a", entity.Name)
	assert.Equal(t, "r1", entity.ReferenceOne.Name)
	assert.True(t, engine.Loaded(entity.ReferenceOne))
	assert.Equal(t, "s1", entity.ReferenceOne.ReferenceTwo.Name)
	assert.True(t, engine.Loaded(entity.ReferenceOne.ReferenceTwo))

	entity = &loadByIDEntity{}
	engine.LoadByID(1, entity)
	engine.Load(entity, "ReferenceOne/ReferenceTwo")
	assert.Equal(t, "r1", entity.ReferenceOne.Name)
	assert.True(t, engine.Loaded(entity.ReferenceOne))
	assert.Equal(t, "s1", entity.ReferenceOne.ReferenceTwo.Name)
	assert.True(t, engine.Loaded(entity.ReferenceOne.ReferenceTwo))

	entityNoCache = &loadByIDNoCacheEntity{}
	found = engine.LoadByID(1, entityNoCache, "*")
	assert.True(t, found)
	assert.Equal(t, uint(1), entityNoCache.ID)
	assert.Equal(t, "a", entityNoCache.Name)

	found = engine.LoadByID(100, entity, "*")
	assert.False(t, found)
	found = engine.LoadByID(100, entity, "*")
	assert.False(t, found)
	entityRedis = &loadByIDRedisEntity{}
	found = engine.LoadByID(100, entityRedis, "*")
	assert.False(t, found)
	found = engine.LoadByID(100, entityRedis, "*")
	assert.False(t, found)

	engine = PrepareTables(t, &Registry{})
	entity = &loadByIDEntity{}
	assert.PanicsWithError(t, "entity 'orm.loadByIDEntity' is not registered", func() {
		engine.LoadByID(1, entity)
	})
}
