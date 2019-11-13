package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"reflect"
	"testing"
	"time"
)

type AddressSchema struct {
	Street   string
	Building uint16
}

type TestEntitySchema struct {
	Orm                  *orm.ORM `orm:"table=TestGetAlters;mysql=schema"`
	Id                   uint
	Name                 string `orm:"length=100;index=FirstIndex"`
	BigName              string `orm:"length=max"`
	Uint8                uint8  `orm:"unique=SecondIndex:2,ThirdIndex"`
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
	Float64              float64
	Float32Decimal       float32  `orm:"decimal=8,2"`
	Float64DecimalSigned float64  `orm:"decimal=8,2;unsigned=false"`
	Enum                 string   `orm:"enum=aaa,bbb,ccc"`
	Set                  []string `orm:"set=vv,hh,dd"`
	Year                 uint16   `orm:"year=true"`
	Date                 time.Time
	DateTime             time.Time `orm:"time=true"`
	Address              AddressSchema
	Json                 interface{}
	ReferenceOneId       *orm.ReferenceOne `orm:"ref=tests.TestEntitySchema"`
}

func TestGetAlters(t *testing.T) {

	orm.UnregisterMySqlPools()
	orm.RegisterMySqlPool("schema", "root:root@tcp(localhost:3310)/test_schema")

	var entity TestEntitySchema
	orm.RegisterEntity(entity)
	tableSchema := orm.GetTableSchema(reflect.TypeOf(entity))
	tableSchema.DropTable()

	safeAlters, unsafeAlters := orm.GetAlters()
	assert.Len(t, safeAlters, 1)
	assert.Len(t, unsafeAlters, 0)
}
