package orm

import (
	"testing"

	apexLog "github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

type loadByIDEntity struct {
	ORM             `orm:"localCache;redisCache"`
	ID              uint
	Name            string `orm:"max=100"`
	ReferenceOne    *loadByIDReference
	ReferenceSecond *loadByIDReference
	ReferenceMany   []*loadByIDReference
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
	ORM            `orm:"localCache;redisCache"`
	ID             uint
	Name           string
	ReferenceTwo   *loadByIDSubReference
	ReferenceThree *loadByIDSubReference2
}

type loadByIDSubReference struct {
	ORM  `orm:"localCache;redisCache"`
	ID   uint
	Name string
}

type loadByIDSubReference2 struct {
	ORM          `orm:"localCache"`
	ID           uint
	Name         string
	ReferenceTwo *loadByIDSubReference
}

func TestLoadById(t *testing.T) {
	var entity *loadByIDEntity
	var entityRedis *loadByIDRedisEntity
	var entityNoCache *loadByIDNoCacheEntity
	var reference *loadByIDReference
	var subReference *loadByIDSubReference
	var subReference2 *loadByIDSubReference2
	engine := PrepareTables(t, &Registry{}, 5, entity, entityRedis, entityNoCache, reference, subReference, subReference2)

	e := &loadByIDEntity{Name: "a", ReferenceOne: &loadByIDReference{Name: "r1", ReferenceTwo: &loadByIDSubReference{Name: "s1"}}}
	e.ReferenceSecond = &loadByIDReference{Name: "r11", ReferenceTwo: &loadByIDSubReference{Name: "s1"},
		ReferenceThree: &loadByIDSubReference2{Name: "s11", ReferenceTwo: &loadByIDSubReference{Name: "hello"}}}
	engine.FlushMany(e,
		&loadByIDEntity{Name: "b", ReferenceOne: &loadByIDReference{Name: "r2", ReferenceTwo: &loadByIDSubReference{Name: "s2"}}},
		&loadByIDEntity{Name: "c"}, &loadByIDNoCacheEntity{Name: "a"})

	engine.FlushMany(&loadByIDReference{Name: "rm1", ID: 100}, &loadByIDReference{Name: "rm2", ID: 101}, &loadByIDReference{Name: "rm3", ID: 102})
	engine.FlushMany(&loadByIDEntity{Name: "eMany", ID: 200, ReferenceMany: []*loadByIDReference{{ID: 100}, {ID: 101}, {ID: 102}}})

	entity = &loadByIDEntity{}
	localLogger := memory.New()
	engine.AddQueryLogger(localLogger, apexLog.InfoLevel, QueryLoggerSourceLocalCache)
	engine.LoadByID(1, entity, "ReferenceOne/ReferenceTwo",
		"ReferenceSecond/ReferenceTwo", "ReferenceSecond/ReferenceThree/ReferenceTwo")
	assert.Len(t, localLogger.Entries, 5)
	assert.True(t, e.Loaded())
	assert.True(t, e.ReferenceOne.Loaded())
	assert.True(t, e.ReferenceOne.ReferenceTwo.Loaded())
	assert.True(t, e.ReferenceSecond.Loaded())
	assert.True(t, e.ReferenceSecond.ReferenceTwo.Loaded())
	assert.True(t, e.ReferenceSecond.ReferenceThree.Loaded())
	assert.True(t, e.ReferenceSecond.ReferenceThree.ReferenceTwo.Loaded())

	entity = &loadByIDEntity{}
	found := engine.LoadByID(1, entity, "ReferenceOne/ReferenceTwo")
	assert.True(t, found)
	assert.Equal(t, uint(1), entity.ID)
	assert.Equal(t, "a", entity.Name)
	assert.Equal(t, "r1", entity.ReferenceOne.Name)
	assert.True(t, entity.ReferenceOne.Loaded())
	assert.Equal(t, "s1", entity.ReferenceOne.ReferenceTwo.Name)
	assert.True(t, entity.ReferenceOne.ReferenceTwo.Loaded())

	entity = &loadByIDEntity{}
	engine.LoadByID(1, entity)
	engine.Load(entity, "ReferenceOne/ReferenceTwo")
	assert.Equal(t, "r1", entity.ReferenceOne.Name)
	assert.True(t, entity.ReferenceOne.Loaded())
	assert.Equal(t, "s1", entity.ReferenceOne.ReferenceTwo.Name)
	assert.True(t, entity.ReferenceOne.ReferenceTwo.Loaded())

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

	entity = &loadByIDEntity{}
	found = engine.LoadByID(200, entity, "ReferenceMany")
	assert.True(t, found)
	assert.Len(t, entity.ReferenceMany, 3)
	assert.Equal(t, uint(100), entity.ReferenceMany[0].ID)
	assert.Equal(t, uint(101), entity.ReferenceMany[1].ID)
	assert.Equal(t, uint(102), entity.ReferenceMany[2].ID)
	assert.Equal(t, "rm1", entity.ReferenceMany[0].Name)
	assert.Equal(t, "rm2", entity.ReferenceMany[1].Name)
	assert.Equal(t, "rm3", entity.ReferenceMany[2].Name)
	assert.True(t, entity.ReferenceMany[0].Loaded())
	assert.True(t, entity.ReferenceMany[1].Loaded())
	assert.True(t, entity.ReferenceMany[2].Loaded())

	engine = PrepareTables(t, &Registry{}, 5)
	entity = &loadByIDEntity{}
	assert.PanicsWithError(t, "entity 'orm.loadByIDEntity' is not registered", func() {
		engine.LoadByID(1, entity)
	})
}

func BenchmarkLoadByIdLocalCache(b *testing.B) {
	entity := &schemaEntity{}
	ref := &schemaEntityRef{}
	registry := &Registry{}
	registry.RegisterEnumStruct("orm.TestEnum", TestEnum)
	registry.RegisterLocalCache(10000)
	engine := PrepareTables(nil, registry, 5, entity, ref)
	e := &schemaEntity{}
	e.Name = "Name"
	e.Uint32 = 1
	e.Int32 = 1
	e.Int8 = 1
	e.Enum = TestEnum.A
	e.RefOne = &schemaEntityRef{}
	engine.Flush(e)
	_ = engine.LoadByID(1, e)
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		_ = engine.LoadByID(1, e)
	}
}
