package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"testing"
	"time"
)

type AddressByIdRedis struct {
	Street   string
	Building uint16
}

type TestEntityByIdRedis struct {
	Orm                  *orm.ORM `orm:"redisCache;ttl=10"`
	Id                   uint
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
	Address              AddressByIdRedis
	Json                 interface{}
	ReferenceOne         *orm.ReferenceOne `orm:"ref=tests.TestEntityByIdRedis"`
}

func TestGetByIdRedis(t *testing.T) {
	var entity TestEntityByIdRedis
	PrepareTables(entity)

	found, err := orm.TryById(100, &entity)
	assert.Nil(t, err)
	assert.False(t, found)
	found, err = orm.TryById(100, &entity)
	assert.Nil(t, err)
	assert.False(t, found)

	entity = TestEntityByIdRedis{}
	err = orm.Flush(&entity)
	assert.Nil(t, err)
	assert.False(t, entity.Orm.IsDirty())

	entity.ReferenceOne.Id = 1
	err = orm.Flush(&entity)
	assert.Nil(t, err)
	assert.False(t, entity.Orm.IsDirty())

	DBLogger := &TestDatabaseLogger{}
	orm.GetMysql().RegisterLogger(DBLogger.Logger())

	found, err = orm.TryById(1, &entity, "ReferenceOne")
	assert.Nil(t, err)
	assert.True(t, found)
	assert.NotNil(t, entity)
	assert.NotNil(t, entity.ReferenceOne.Reference)
	assert.Equal(t, uint(1), entity.Id)
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
	assert.Equal(t, AddressByIdRedis{Street: "", Building: uint16(0)}, entity.Address)
	assert.Nil(t, entity.Json)
	assert.False(t, entity.Orm.IsDirty())
	assert.Len(t, DBLogger.Queries, 1)

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
	entity.Json = map[string]string{"name": "John"}

	assert.True(t, entity.Orm.IsDirty())

	err = orm.Flush(&entity)
	assert.Nil(t, err)
	assert.False(t, entity.Orm.IsDirty())

	assert.Len(t, DBLogger.Queries, 2)

	found, err = orm.TryById(1, &entity)
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
	assert.Equal(t, map[string]interface{}{"name": "John"}, entity.Json)
	assert.Len(t, DBLogger.Queries, 3)

	_, err = orm.TryById(1, &entity)
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Queries, 3)
}

func BenchmarkGetById(b *testing.B) {
	var entity TestEntityByIdRedis
	PrepareTables(entity)

	entity = TestEntityByIdRedis{}
	_ = orm.Flush(&entity)

	for n := 0; n < b.N; n++ {
		_, _ = orm.TryById(1, &entity)
	}
}
