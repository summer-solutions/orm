# orm

## Defining database and cache pool connections

First you need to define connections to all databases. You should do it once, 
when your application starts.

```go
package main

import "github.com/summer-solutions/orm"

func main() {

    defer orm.Defer()

    /*MySQL */

    orm.RegisterMySqlPool("root:root@tcp(localhost:3306)/database_name")
    //optionally you can define pool name as second argument
    orm.RegisterMySqlPool("root:root@tcp(localhost:3307)/database_name", "second_pool")

    /* Redis */

    orm.RegisterRedis("localhost:6379", 0) //seconds argument is a redis database number
    //optionally you can define pool name as second argument
    orm.RegisterRedis("localhost:6379", 1, "second_pool")

    /* Redis used to handle queues (explained later) */

    orm.RegisterRedis("localhost:6379", 3, "queues_pool")
    orm.SetRedisForQueue("queues_pool") //if not defined orm is using default redis pool


    /* Local cache (in memory) */

    orm.RegisterLocalCache(1000) //you need to define cache size
    //optionally you can define pool name as second argument
    orm.RegisterLocalCache(100, "second_pool")

    /* Context cache (explain later) */
    orm.EnableContextCache(100, 1)

}

```

## Defining entities

Great, we have required connections defined, now it's time to define our data models.
Simple create struct using special tag "orm":

```go
package main

import (
	"github.com/summer-solutions/orm"
	"time"
)

func main() {

    type Address struct {
    	Street   string
    	Building uint16
    }
    
    type TestEntity struct {
    	Orm                  *orm.ORM
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
    	Address              Address
    	Json                 interface{}
    	ReferenceOne         *orm.ReferenceOne  `orm:"ref=TestEntity"`
    	ReferenceMany        *orm.ReferenceMany `orm:"ref=TestEntity"`
    }

}

```