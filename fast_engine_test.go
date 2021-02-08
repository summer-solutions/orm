package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestFastEngine(t *testing.T) {
	var entity *loadByIDEntity
	var entityRedis *loadByIDRedisEntity
	var entityNoCache *loadByIDNoCacheEntity
	var reference *loadByIDReference
	var subReference *loadByIDSubReference
	var subReference2 *loadByIDSubReference2
	engine := PrepareTables(t, &Registry{}, 5, entity, entityRedis, entityNoCache, reference, subReference, subReference2)
	engine.FlushMany(&loadByIDEntity{Name: "a", ReferenceOne: &loadByIDReference{Name: "r1", ReferenceTwo: &loadByIDSubReference{Name: "s1"}}},
		&loadByIDEntity{Name: "b", ReferenceOne: &loadByIDReference{Name: "r2", ReferenceTwo: &loadByIDSubReference{Name: "s2"}}},
		&loadByIDEntity{Name: "c"}, &loadByIDNoCacheEntity{Name: "a"})
	engine.FlushMany(&loadByIDReference{Name: "rm1", ID: 100}, &loadByIDReference{Name: "rm2", ID: 101}, &loadByIDReference{Name: "rm3", ID: 102})
	engine.FlushMany(&loadByIDEntity{Name: "eMany", ID: 200, ReferenceMany: []*loadByIDReference{{ID: 100}, {ID: 101}, {ID: 102}}})

	fastEngine := engine.NewFastEngine()
	has, fastEntity := fastEngine.LoadByID(1, entity)
	assert.True(t, has)
	assert.NotNil(t, fastEntity)
	assert.Equal(t, uint64(1), fastEntity.GetID())
	assert.Equal(t, "a", fastEntity.Get("Name"))
	assert.True(t, fastEntity.Is(entity))
	assert.False(t, fastEntity.Is(entityRedis))
	assert.PanicsWithError(t, "unknown field Invalid", func() {
		fastEntity.Get("Invalid")
	})
	entity = &loadByIDEntity{}
	fastEntity.Fill(entity)
	assert.Equal(t, "a", entity.Name)
}

func BenchmarkLoadByIdLocalCacheFastEngine(b *testing.B) {
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
	fastEngine := engine.NewFastEngine()
	_, _ = fastEngine.LoadByID(1, e)
	b.ResetTimer()
	b.ReportAllocs()
	// BenchmarkLoadByIdLocalCache-12    	  906270	      1296 ns/op	     296 B/op	       6 allocs/op
	// BenchmarkLoadByIdLocalCacheFastEngine-12    	 4452970	       274 ns/op	      56 B/op	       2 allocs/op
	for n := 0; n < b.N; n++ {
		_, _ = fastEngine.LoadByID(1, e)
	}
}
