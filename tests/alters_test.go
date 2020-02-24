package tests

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"testing"
	"time"
)

type AddressSchema struct {
	Street   string
	Building uint16
}

type fieldsColors struct {
	Red    string
	Green  string
	Blue   string
	Yellow string
	Purple string
}

var Color = &fieldsColors{
	Red:    "Red",
	Green:  "Green",
	Blue:   "Blue",
	Yellow: "Yellow",
	Purple: "Purple",
}

type TestEntitySchema struct {
	Orm                  *orm.ORM `orm:"mysql=schema"`
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
	Enum                 string   `orm:"enum=tests.Color;notnull"`
	Set                  []string `orm:"set=tests.Color"`
	Year                 uint16   `orm:"year=true"`
	Date                 time.Time
	DateTime             time.Time `orm:"time=true"`
	Address              AddressSchema
	Json                 interface{}
	ReferenceOne         *orm.ReferenceOne  `orm:"ref=tests.TestEntitySchema"`
	ReferenceMany        *orm.ReferenceMany `orm:"ref=tests.TestEntitySchema"`
}

func TestGetAlters(t *testing.T) {

	orm.UnregisterSqlPools()
	err := orm.RegisterMySqlPool("root:root@tcp(localhost:3308)/test_schema", "schema")
	assert.Nil(t, err)

	var entity TestEntitySchema
	orm.RegisterEntity(entity)
	orm.RegisterEnum("tests.Color", Color)
	tableSchema := orm.GetTableSchema(entity)
	err = tableSchema.DropTable()
	assert.Nil(t, err)

	alters, err := orm.GetAlters()
	assert.Nil(t, err)
	assert.Len(t, alters, 1)
	fmt.Printf("%v", alters)
}
