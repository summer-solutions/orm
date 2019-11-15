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
    	ReferenceMany        *orm.ReferenceMany `orm:"ref=TestEntity;max=100"`
    }

}
```

There are only two golden rules you need to remember defining entity struct: 

 * first field must have name "Orm" and must be type of "*orm.ORM"
 * second argument must have name "Id" and must be type of one of uint, uint16, uint32, uint64
 
 
 As you can see orm is not using null values like sql.NullString. Simply set empty string "" and orm will
 convert it to null in database. 
 
 ## Updating schema
 
 ```go
 package main
 
 import "github.com/summer-solutions/orm"
 
 func main() {
 
     defer orm.Defer()
     orm.RegisterMySqlPool("root:root@tcp(localhost:3306)/database_name")
    
     type FirstEntity struct {
        Orm                  *orm.ORM
        Id                   uint
        Name                 string
     }
      
    type SecondEntity struct {
        Orm                  *orm.ORM
        Id                   uint
        Name                 string
    }
    
    var firstEntity  FirstEntity
    var secondEntity SecondEntity
	orm.RegisterEntity(firstEntity, secondEntity)
    
    safeAlters, unsafeAlters := orm.GetAlters()
    
    /* in safeAlters and unsafeAlters you can find all sql queries (CREATE, DROP, ALTER TABLE) that needs
    to be executed based on registered entities. "safeAlters" you can execute without any stress,
    no data will be lost. But be careful executing queries from "unsafeAlters". You can loose some data, 
    e.g. table needs to be dropped that contains some rows. */
    
    /*optionally you can execute alters for each model*/
    orm.GetTableSchema(firstEntity).UpdateSchema() //it will create or alter table if needed
    orm.GetTableSchema(firstEntity).DropTable() //it will drop table if exist
    has, safeAlters, unsafeAlters := orm.GetTableSchema(firstEntity).GetSchemaChanges() //if you need to what needs to be executed

 }
 
 ```

 ## Logging
 
 ```go
 package main
 
 import "github.com/summer-solutions/orm"
 
 func main() {
 
     defer orm.Defer()
     orm.RegisterMySqlPool("root:root@tcp(localhost:3306)/database_name")
     orm.RegisterMySqlPool("root:root@tcp(localhost:3307)/database_name", "second_pool")
     orm.RegisterRedis("localhost:6379", 0)
     orm.RegisterLocalCache(1000)
     orm.EnableContextCache(100, 1)
   
     /*to enable simple logger that prints queries to standard output*/
     dbLogger := orm.StandardDatabaseLogger{}
     orm.GetMysql().AddLogger(dbLogger)
     orm.GetMysql("second_pool").AddLogger(dbLogger)
    
     cacheLogger := orm.StandardCacheLogger{}
     orm.GetRedis().AddLogger(cacheLogger)   
     orm.GetLocalCache().AddLogger(cacheLogger)
     orm.GetContextCache().AddLogger(cacheLogger)
    
    /*defining your own logger*/
    type MyDatabaseLogger struct {
    }

    func (l *MyDatabaseLogger) Log(mysqlCode string, query string, args ...interface{}) {
    }
    type MyCacheLogger struct {
    }

    func (l *MyCacheLogger) Log(cacheType string, code string, key string, operation string, misses int) {
    }
 }
 
 ```
