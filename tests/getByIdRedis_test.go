package tests

import (
	"testing"
	"time"

	"github.com/apex/log"

	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

type AddressByIDRedis struct {
	Street   string
	Building uint16
}

type TestEntityByIDRedis struct {
	orm.ORM              `orm:"redisCache;ttl=10"`
	ID                   uint
	Name                 string `orm:"length=100;index=FirstIndex"`
	BigName              string `orm:"length=max"`
	Uint8                uint8  `orm:"unique=SecondIndex:2,ThirdIndex"`
	Uint16               uint16
	Uint24               uint32 `orm:"mediumint=true"`
	Uint32               uint32
	Uint64               uint64 `orm:"unique=SecondIndex"`
	Int8                 int8
	Int16                int16
	Int32                int32
	Int64                int64
	Rune                 rune
	Int                  int
	Bool                 bool
	Float32              float32
	Float32Precision     float32 `orm:"precision=10"`
	Float64              float64
	Float32Decimal       float32 `orm:"decimal=8,2"`
	Float64DecimalSigned float64 `orm:"decimal=8,4;unsigned=false"`
	Year                 uint16  `orm:"year=true"`
	Date                 time.Time
	DateTime             time.Time `orm:"time=true"`
	Address              AddressByIDRedis
	JSON                 interface{}
	ReferenceOne         *TestEntityByIDRedis
}

func TestGetByIDRedis(t *testing.T) {
	var entity TestEntityByIDRedis
	engine := PrepareTables(t, &orm.Registry{}, entity)

	found, err := engine.LoadByID(100, &entity)
	assert.Nil(t, err)
	assert.False(t, found)
	found, err = engine.LoadByID(100, &entity)
	assert.Nil(t, err)
	assert.False(t, found)

	entity = TestEntityByIDRedis{}
	engine.Track(&entity)
	err = engine.Flush()
	assert.Nil(t, err)
	assert.False(t, engine.IsDirty(&entity))

	var entity2 TestEntityByIDRedis
	has, err := engine.LoadByID(1, &entity2)
	assert.True(t, has)
	assert.Nil(t, err)
	assert.Equal(t, uint(1), entity2.ID)
	assert.Nil(t, entity2.ReferenceOne)

	var entity3 TestEntityByIDRedis
	has, err = engine.LoadByID(1, &entity3)
	assert.True(t, has)
	assert.Nil(t, err)
	assert.Equal(t, uint(1), entity3.ID)

	engine.Track(&entity)
	entity.ReferenceOne = &TestEntityByIDRedis{ID: 1}
	err = engine.Flush()
	assert.Nil(t, err)
	assert.False(t, engine.IsDirty(&entity))

	DBLogger := memory.New()
	pool := engine.GetMysql()
	assert.True(t, has)
	pool.AddLogger(DBLogger)
	pool.SetLogLevel(log.InfoLevel)

	found, err = engine.LoadByID(1, &entity, "ReferenceOne")
	assert.Nil(t, err)
	assert.True(t, found)
	assert.NotNil(t, entity)
	assert.True(t, engine.Loaded(entity.ReferenceOne))
	assert.Equal(t, uint(1), entity.ID)
	assert.Equal(t, "", entity.Name)
	assert.Equal(t, "", entity.BigName)
	assert.Equal(t, uint8(0), entity.Uint8)
	assert.Equal(t, uint16(0), entity.Uint16)
	assert.Equal(t, uint32(0), entity.Uint24)
	assert.Equal(t, uint32(0), entity.Uint32)
	assert.Equal(t, int8(0), entity.Int8)
	assert.Equal(t, int16(0), entity.Int16)
	assert.Equal(t, int32(0), entity.Int32)
	assert.Equal(t, int64(0), entity.Int64)
	assert.Equal(t, rune(0), entity.Rune)
	assert.Equal(t, 0, entity.Int)
	assert.Equal(t, false, entity.Bool)
	assert.Equal(t, float32(0), entity.Float32)
	assert.Equal(t, float64(0), entity.Float64)
	assert.Equal(t, float32(0), entity.Float32Decimal)
	assert.Equal(t, float64(0), entity.Float64DecimalSigned)
	assert.Equal(t, uint16(0), entity.Year)
	assert.IsType(t, time.Time{}, entity.Date)
	assert.True(t, entity.Date.Equal(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)))
	assert.IsType(t, time.Time{}, entity.DateTime)
	assert.True(t, entity.Date.Equal(time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC)))
	assert.Equal(t, AddressByIDRedis{Street: "", Building: uint16(0)}, entity.Address)
	assert.Nil(t, entity.JSON)
	assert.False(t, engine.IsDirty(&entity))
	assert.Len(t, DBLogger.Entries, 1)

	found, err = engine.LoadByID(1, &entity, "ReferenceOne")
	assert.Nil(t, err)
	assert.True(t, found)
	assert.True(t, engine.Loaded(entity.ReferenceOne))

	engine.Track(&entity)
	entity.Name = "Test name"
	entity.BigName = "Test big name"
	entity.Uint8 = 2
	entity.Int = 14
	entity.Bool = true
	entity.Float32 = 1.11
	entity.Float32Precision = 1.13
	entity.Float64 = 7.002
	entity.Float32Decimal = 123.13
	entity.Float64DecimalSigned = -12.01
	entity.Year = 1982
	entity.Date = time.Date(1982, 4, 6, 0, 0, 0, 0, time.UTC)
	entity.DateTime = time.Date(2019, 2, 11, 12, 34, 11, 0, time.UTC)
	entity.Address.Street = "wall street"
	entity.Address.Building = 12
	entity.JSON = map[string]string{"name": "John"}

	assert.True(t, engine.IsDirty(&entity))

	err = engine.Flush()
	assert.Nil(t, err)
	assert.False(t, engine.IsDirty(&entity))

	assert.Len(t, DBLogger.Entries, 2)

	found, err = engine.LoadByID(1, &entity)
	assert.Nil(t, err)
	assert.True(t, found)
	assert.NotNil(t, entity)
	assert.Equal(t, "Test name", entity.Name)
	assert.Equal(t, "Test big name", entity.BigName)
	assert.Equal(t, uint8(2), entity.Uint8)
	assert.Equal(t, 14, entity.Int)
	assert.Equal(t, true, entity.Bool)
	assert.Equal(t, float32(1.11), entity.Float32)
	assert.Equal(t, float32(1.13), entity.Float32Precision)
	assert.Equal(t, 7.002, entity.Float64)
	assert.Equal(t, float32(123.13), entity.Float32Decimal)
	assert.Equal(t, -12.01, entity.Float64DecimalSigned)
	assert.Equal(t, uint16(1982), entity.Year)
	assert.Equal(t, time.Date(1982, 4, 6, 0, 0, 0, 0, time.UTC), entity.Date)
	assert.Equal(t, time.Date(2019, 2, 11, 12, 34, 11, 0, time.UTC), entity.DateTime)
	assert.Equal(t, "wall street", entity.Address.Street)
	assert.Equal(t, uint16(12), entity.Address.Building)
	assert.Equal(t, map[string]interface{}{"name": "John"}, entity.JSON)
	assert.Len(t, DBLogger.Entries, 3)

	_, err = engine.LoadByID(1, &entity)
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Entries, 3)
}

func BenchmarkLoadByID(b *testing.B) {
	var entity *TestEntityByIDRedis
	engine := PrepareTables(&testing.T{}, &orm.Registry{}, entity)

	entity = &TestEntityByIDRedis{}
	engine.Track(entity)
	err := engine.Flush()
	assert.Nil(b, err)

	for n := 0; n < b.N; n++ {
		_, _ = engine.LoadByID(1, entity)
	}
}
