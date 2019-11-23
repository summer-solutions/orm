# orm

## Defining database and cache pool connections

First you need to define connections to all databases. You should do it once, 
when your application starts.

```go
package main

import "github.com/summer-solutions/orm"

func main() {

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

You can also configure orm using yaml configuration file:

```.yaml

orm:
  default:
    mysql: root:root@tcp(localhost:3310)/db
    redis: localhost:6379:0
    redisQueues: localhost:6379:1
    localCache: 1000
    contextCache: 1000
  second_pool:
    mysql: root:root@tcp(localhost:3311)/db2
    redis: localhost:6380:1 

```

```go
package main

import (
    "github.com/summer-solutions/orm"
    "gopkg.in/yaml.v2"
    "io/ioutil"
)

func main() {

    yamlFileData, err := ioutil.ReadFile("./orm.yaml")
    if err != nil {
        //...
    }
    
    var parsedYaml map[interface{}]interface{}
    err = yaml.Unmarshal(yamlFileData, &parsedYaml)
    err = orm.InitByYaml(parsedYaml)
    if err != nil {
        //...
    }

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
    type TestEntitySecondPool struct {
    	Orm                  *orm.ORM `orm:"mysql=second_pool"`
    	Id                   uint
    }
}
```

There are only two golden rules you need to remember defining entity struct: 

 * first field must have name "Orm" and must be type of "*orm.ORM"
 * second argument must have name "Id" and must be type of one of uint, uint16, uint32, uint64
 
 
 As you can see orm is not using null values like sql.NullString. Simply set empty string "" and orm will
 convert it to null in database. 
 
 By default entity is not cached in local cache or redis, to change that simply use key "redisCache" or "localCache"
 in "orm" tag for "Orm" field:
 
 ```go
 package main
 
 import (
 	"github.com/summer-solutions/orm"
 	"time"
 )
 
 func main() {
 
     type TestEntityLocalCache struct {
     	Orm                  *orm.ORM `orm:"localCache"` //default pool
        //...
     }
    
    type TestEntityLocalCacheSecondPool struct {
     	Orm                  *orm.ORM `orm:"localCache=second_pool"`
        //...
     }
    
    type TestEntityRedisCache struct {
     	Orm                  *orm.ORM `orm:"redisCache"` //default pool
        //...
     }
    
    type TestEntityRedisCacheSecondPool struct {
     	Orm                  *orm.ORM `orm:"redisCache=second_pool"`
        //...
     }

    type TestEntityLocalAndRedisCache struct {
     	Orm                  *orm.ORM `orm:"localCache;redisCache"`
        //...
     }
 }
 ```
 
 ## Updating schema
 
 ```go
 package main
 
 import "github.com/summer-solutions/orm"
 
 func main() {
 
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
    
    safeAlters, unsafeAlters, err := orm.GetAlters()

    
    /* in safeAlters and unsafeAlters you can find all sql queries (CREATE, DROP, ALTER TABLE) that needs
    to be executed based on registered entities. "safeAlters" you can execute without any stress,
    no data will be lost. But be careful executing queries from "unsafeAlters". You can loose some data, 
    e.g. table needs to be dropped that contains some rows. */
    
    /*optionally you can execute alters for each model*/
    orm.GetTableSchema(firstEntity).UpdateSchema() //it will create or alter table if needed
    orm.GetTableSchema(firstEntity).DropTable() //it will drop table if exist
    //if you need to see queries:
    has, safeAlters, unsafeAlters, err := orm.GetTableSchema(firstEntity).GetSchemaChanges()
 }
 
 ```

 ## Logging
 
 ```go
 package main
 
 import "github.com/summer-solutions/orm"
 
 func main() {
 
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
    func (l *MyDatabaseLogger) Log(mysqlCode string, query string, microseconds int64, args ...interface{}) {
    }

    type MyCacheLogger struct {
    }
    func (l *MyCacheLogger) Log(cacheType string, code string, key string, operation string, microseconds int64, misses int) {
    }
    
    /* adding logger to all pools */
    orm.AddDatabaseLogger(dbLogger)
    orm.AddRedisLogger(cacheLogger)

 }
 
 ```


## Adding, editing, deleting entities

```go
package main

import "github.com/summer-solutions/orm"

func main() {

    orm.RegisterMySqlPool("root:root@tcp(localhost:3306)/database_name")

    type TestEntity struct {
        Orm                  *orm.ORM
        Id                   uint
        Name                 string
    }
    var entity TestEntity
    orm.RegisterEntity(entity)
    //code above you should execute only once, when application starts
    
    entity = TestEntity{Name: "Name 1"}
    orm.Init(&entity) // you should use this function only for new entities
    err := orm.Flush(&entity)
    if err != nil {
       ///...
    }

    /*if you need to add more than one entity*/
    entity = TestEntity{Name: "Name 2"}
    entity2 := TestEntity{Name: "Name 3"}
    orm.Init(&entity, &entity2)
    //it will execute only one query in MySQL adding two rows at once (atomic)
    err = orm.Flush(&entity, &entity2)
    if err != nil {
       ///...
    }

    /* editing */
    entity.Name = "New name 2"
    entity.Orm.IsDirty() //returns true
    entity2.Orm.IsDirty() //returns false
    err = orm.Flush(&entity)
    if err != nil {
       ///...
    }
    entity.Orm.IsDirty() //returns false
    
    /* deleting */
    entity2.Orm.MarkToDelete()
    entity2.Orm.IsDirty() //returns true
    err = orm.Flush(&entity)
    if err != nil {
       ///...
    }
}

```

If you need to work with more than one entity i strongly recommend ot use FLusher (described later).


## Getting entities using primary keys

```go
package main

import "github.com/summer-solutions/orm"

func main() {

   //.. register pools and entities
 
    type TestEntity struct {
        Orm                  *orm.ORM
        Id                   uint
        Name                 string
    }
    var entity TestEntity
    found, err := orm.TryById(1, &entity) //found has false if row does not exists
    orm.GetById(2, &entity) //will panic if row does not exist

    var entities []TestEntity
    //missing is []uint64 that contains id of rows that doesn't exists, 
    // in this cause $found slice has nil for such keys
    missing, err := orm.TryByIds([]uint64{2, 3, 1}, &entities) 
    orm.GetByIds([]uint64{2, 3, 1}, &entities) //will panic if at least one row does not exist
}

```

## Getting entities using search

```go
package main

import "github.com/summer-solutions/orm"

func main() {

   //.. register pools and entities
 
    type TestEntity struct {
        Orm                  *orm.ORM
        Id                   uint
        Name                 string
    }

    var entities []TestEntity
    pager := orm.Pager{CurrentPage: 1, PageSize: 100}
    where := orm.NewWhere("`Id` > ? AND `Id` < ?", 1, 8)
    err := orm.Search(where, pager, &entities)
    
    //or if you need number of total rows
    totalRows, err := orm.SearchWithCount(where, pager, &entities)
    
    //or if you need only one row
    where := orm.NewWhere("`Name` = ?", "Hello")
    var entity TestEntity
    found, err := orm.SearchOne(where, &entity)
    
    //or if you need only primary keys
    ids, err := orm.SearchIds(where, pager, entity)
    
    //or if you need only primary keys and total rows
    ids, totalRows, err = orm.SearchIdsWithCount(where, pager, entity)
}

```

## Flusher

Very often you need to change more than one entity. It's hard to track all of them
to see if some of them are dirty. Also it's better to update them at the same time trying
to minimize number of requests to database and cache. To solve this problem simple use FLusher:

```go
package main

import "github.com/summer-solutions/orm"

func main() {

   //.. register pools and entities
 
    type TestEntity struct {
        Orm                  *orm.ORM
        Id                   uint
        Name                 string
    }

    /* In this case flusher will keep maximum 100 entities. If you add more it will panic */
    flusher := orm.NewFlusher(100, false)

    var entity1 TestEntity
    var entity2 TestEntity
    entity3 := TestEntity{Name: "Hello"}
    err := orm.GetById(1, &entity1)
    err = orm.GetById(2, &entity2)
    err = flusher.RegisterEntity(&entity1, &entity2, &entity3)
   
    entity1.Name = "New Name"
    entity1.Orm.MarkToDelete()
    
    err = flusher.Flush() //executes all queries at once

     /* 
        in this case flusher will keep maximum 1000 entities. 
        If you add more it automatically flush all of them and unregister them in flusher 
    */
    flusher = orm.NewFlusher(1000, true)
    
    var entities []TestEntity
    pager := orm.Pager{CurrentPage: 1, PageSize: 100}
    where := orm.NewWhere("1")
    for {
        err = orm.Search(where, pager, &entities)
        for _, entity := range entities {
          entity.Name = "New Name"
          err = flusher.RegisterEntity(&entity) //it will auto flush every 10 iterations
        }
        pager.IncrementPage()
        if len(entities) < pager.GetPageSize() {
            break
        }
    }
    err = flusher.Flush()
    if err != nil {
       ///...
    }

}

```


## Reference one to one

```go
package main

import "github.com/summer-solutions/orm"

func main() {

   //.. register pools and entities
 
    type UserEntity struct {
        Orm                  *orm.ORM
        Id                   uint64
        Name                 string
        School               *orm.ReferenceOne  `orm:"ref=SchoolEntity"`
    }
    
    type SchoolEntity struct {
        Orm                  *orm.ORM
        Id                   uint64
        Name                 string
    }
    
    school := SchoolEntity{Name: "Name of school"}
    err := orm.Flush(&school)
    if err != nil {
       ///...
    }
    
    user := UserEntity{Name: "John"}
    user.School.Id = school.Id
    err = orm.Flush(&user)
    if err != nil {
       ///...
    }

    /* accessing reference */
    user.School.Has() //returns true
    has, err = :user.School.Load(&school) //has is true
    
    /* deleting reference */
    user.School.Id = 0
    err = orm.Flush(&user)
    if err != nil {
       ///...
    }

    /* if you don't have ID yet you can still assign references */
    school = SchoolEntity{Name: "New School"}
    user.School.Reference = &school
    err = orm.Flush(&user, &school)
    if err != nil {
       ///...
    }

}

```

## Reference one to many


```go
package main

import "github.com/summer-solutions/orm"

func main() {

   //.. register pools and entities
 
    type UserEntity struct {
        Orm                  *orm.ORM
        Id                   uint64
        Name                 string
        Addresses            *orm.ReferenceMany  `orm:"ref=AddressEntity"`
    }
    
    type AddressEntity struct {
        Orm                  *orm.ORM
        Id                   uint64
        City                 string
        Street               string
    }
    
    address1 := AddressEntity{City: "New York", Street: "Times Square 12"}
    address2 := AddressEntity{City: "Boston", Street: "Main 1a"}
    orm.Init(&address1, &address2)
    err := orm.Flush(&address1, &address2)
    
    user := UserEntity{Name: "John"}
    orm.Init(&user)
    user.Addresses.Add(address1.Id, address2.Id)
    err = orm.Flush(&user)

    /* accessing reference */
    user.Addresses.Has(address1.Id) //returns true
    user.Addresses.Len() //returns 2
    var addresses []AddressEntity
    err = user.Addresses.Load(&addresses) //has is true
    
    /* deleting reference */
    user.Addresses.Clear()
    //or
    user.Addresses.Remove(1, 2)

    /* if you don't have ID yet you can still assign references */
    newAddress := AddressEntity{City: "Boston", Street: "Main 12"}
    user.Addresses.AddReference(&newAddress)
    err = orm.Flush(&user, &newAddress)
}

```


It's a good practice to use one to many reference 
only if you are connecting no more than 50 rows. As you can se you can't use pager here so
all rows are loaded at once. If you need to work with more rows you should use cached
queries (explained below).

## Cached queries

```go
package main

import "github.com/summer-solutions/orm"

func main() {

   //.. register pools and entities
 
    type UserEntity struct {
        Orm                  *orm.ORM
        Id                   uint64
        Name                 string
        Age                  uint16
        IndexAge             *orm.CachedQuery `query:":Age = ? ORDER BY :Id"`
        IndexAll             *orm.CachedQuery `query:""`
    }
    
    user := UserEntity{Name: "John", Age: 18}
    err := orm.Flush(&user)
    pager := orm.Pager{CurrentPage: 1, PageSize: 100}
    var users []UserEntity
    totalRows, err := orm.CachedSearch(&users, "IndexAge", pager, 18)
    totalRows, err = orm.CachedSearch(&users, "IndexAll", pager)

}

```

Beauty about cached cached queries is that you don't need to care about updating
cache when entity is changed. Results are cached and updated automatically.

## Lazy flush

Sometimes you want to flush changes in database, but it's ok if data is flushed
asynchronously. Just use LazyFlush and LazyReceiver:

```go
package main

import "github.com/summer-solutions/orm"

func main() {

    orm.RegisterMySqlPool("root:root@tcp(localhost:3306)/database_name")
    orm.RegisterRedis("localhost:6379", 3, "queues_pool")
    orm.SetRedisForQueue("queues_pool") //if not defined orm is using default redis pool
    //.. register entities

 
    type UserEntity struct {
        Orm                  *orm.ORM
        Id                   uint64
        Name                 string
    }
    
    user := UserEntity{Name: "John"}
    var user2 UserEntity
    err := orm.GetById(1, &user2)
    user2.Orm.MarkToDelete()
    err = orm.FlushLazy(&user, &user2)
    
    /* you can use Flusher also */
    flusher := orm.NewLazyFlusher(100, true)
    user = UserEntity{Name: "Bob"}
    err = flusher.RegisterEntity(&user)
    err = flusher.Flush()
    if err != nil {
       ///...
    }
    
    /* you should run a thread that is receiving lazy queries */
    lazyReceiver := orm.LazyReceiver{RedisName: "queues_pool"}
    for {
        err = lazyReceiver.Digest()
        if err != nil {
           ///...
        }
        //sleep x seconds
    }
}

```

## Flush in cache

Sometimes you are changing entity very often and you don't want to flush changes
to database every time entity is changed. If entity is using shared cached (redis) you
can use FlushInCache feature. When entity is changed new data is stored in cache but 
from time to time FlushInCacheReceiver is flushing all differences between cache and database.

```go
package main

import "github.com/summer-solutions/orm"

func main() {

    orm.RegisterMySqlPool("root:root@tcp(localhost:3306)/database_name")
    orm.RegisterRedis("localhost:6379", 3, "queues_pool")
    orm.SetRedisForQueue("queues_pool") //if not defined orm is using default redis pool
    //.. register entities

 
    type UserEntity struct {
        Orm                  *orm.ORM
        Id                   uint64
        Name                 string
    }
    
    var user UserEntity
    err := orm.GetById(1, &user)
    user.Name = "New name"
    err = orm.FlushInCache(&user) //updated only in redis
    user.Name = "New name 2"
    err = orm.FlushInCache(&user) //updated only in redis
    
    /* you should run a thread that is flushing changes in database */
    lazyReceiver := orm.FlushInCacheReceiver{RedisName: "queues_pool"}
    for {
        //in our case it will only one query:
        // UPDATE `UserEntity` SET `Name` = "New name 2" WHERE `Id` = 1
        err = lazyReceiver.Digest()
        //sleep x seconds
    }
}

```

## Working with Redis

```go
package main

import "github.com/summer-solutions/orm"

func main() {

    orm.RegisterRedis("localhost:6379", 0)
    
    //storing data in cached for x seconds
    val, err := orm.GetRedis().GetSet("key", 1, func() interface{} {
		return "hello"
	})
    
    //standard redis api
    keys, err := orm.GetRedis().LRange("key", 1, 2)
    err = orm.GetRedis().LPush("key", "a", "b")
    //...
}

```

## Working with local cache

```go
package main

import "github.com/summer-solutions/orm"

func main() {

    orm.RegisterLocalCache(1000)
    
    //storing data in cached for x seconds
    val := orm.GetLocalCache().GetSet("key", 1, func() interface{} {
        return "hello"
    })
    
    //getting value
    value, has := orm.GetLocalCache().Get("key")
    
    //getting many values
    values := orm.GetLocalCache().MGet("key1", "key2")
    
    //setting value
    orm.GetLocalCache().Set("key", "value")
    
    //setting values
    orm.GetLocalCache().MSet("key1", "value1", "key2", "value2" /*...*/)
    
    //getting values from hash set (like redis HMGET)
    values := orm.GetLocalCache().HMget("key")
    
    //setting values in hash set
    orm.GetLocalCache().HMset("key", map[string]interface{}{"key1" : "value1", "key2": "value 2"})

    //deleting value
    orm.GetLocalCache().Remove("key1", "key2" /*...*/)
    
    //clearing cache
    orm.GetLocalCache().Clear()

}

```

## Working with mysql

```go
package main

import (
    "database/sql"
    "github.com/summer-solutions/orm"
)

func main() {

    orm.RegisterMySqlPool("root:root@tcp(localhost:3306)/database_name")

    res, err := orm.GetMysql().Exec("UPDATE `table_name` SET `Name` = ? WHERE `Id` = ?", "Hello", 2)

    var row string
    err = orm.GetMysql().QueryRow("SELECT * FROM `table_name` WHERE  `Id` = ?", 1).Scan(&row)
    if err != nil {
        if err != sql.ErrNoRows {
            ///...
        }
        //no row found
    }
    
    results, err := orm.GetMysql().Query("SELECT * FROM `table_name` WHERE  `Id` > ? LIMIT 100", 1)
    for results.Next() {
    	var row string
        err = results.Scan(&row)
    }

}

```