# orm

![Check & test](https://github.com/summer-solutions/orm/workflows/Check%20&%20test/badge.svg)
[![codecov](https://codecov.io/gh/summer-solutions/orm/branch/master/graph/badge.svg)](https://codecov.io/gh/summer-solutions/orm)
[![Go Report Card](https://goreportcard.com/badge/github.com/summer-solutions/orm)](https://goreportcard.com/report/github.com/summer-solutions/orm)
[![MIT license](https://img.shields.io/badge/license-MIT-brightgreen.svg)](https://opensource.org/licenses/MIT)



ORM that delivers support for full stack data access:

 * MySQL - for relational data
 * Redis - for NoSQL in memory shared cache
 * Elastic Search - for full text search
 * Local Cache - in memory local (not shared) cache
 * ClickHouse - time series database
 * RabbitMQ - message broker
 * DataDog - monitoring
 
Menu:

 * [Configuration](https://github.com/summer-solutions/orm#configuration) 
 * [Defining entities](https://github.com/summer-solutions/orm#defining-entities) 
 * [Creating registry](https://github.com/summer-solutions/orm#validated-registry) 
 * [Creating engine](https://github.com/summer-solutions/orm#creating-engine) 
 * [Checking and updating table schema](https://github.com/summer-solutions/orm#checking-and-updating-table-schema) 
 * [Adding, editing, deleting entities](https://github.com/summer-solutions/orm#adding-editing-deleting-entities) 
 * [Transactions](https://github.com/summer-solutions/orm#transactions) 
 * [Loading entities using primary key](https://github.com/summer-solutions/orm#loading-entities-using-primary-key) 
 * [Loading entities using search](https://github.com/summer-solutions/orm#loading-entities-using-search) 
 * [Reference one to one](https://github.com/summer-solutions/orm#reference-one-to-one) 
 * [Cached queries](https://github.com/summer-solutions/orm#cached-queries) 
 * [Lazy flush](https://github.com/summer-solutions/orm#lazy-flush)
 * [Data loader](https://github.com/summer-solutions/orm#data-loader) 
 * [Log entity changes](https://github.com/summer-solutions/orm#log-entity-changes) 
 * [Dirty stream](https://github.com/summer-solutions/orm#dirty-stream) 
 * [Fake delete](https://github.com/summer-solutions/orm#fake-delete) 
 * [Working with Redis](https://github.com/summer-solutions/orm#working-with-redis) 
 * [Working with local cache](https://github.com/summer-solutions/orm#working-with-local-cache) 
 * [Working with mysql](https://github.com/summer-solutions/orm#working-with-mysql) 
 * [Working with elastic search](https://github.com/summer-solutions/orm#working-with-elastic-search)  
 * [Working with ClickHouse](https://github.com/summer-solutions/orm#working-with-clickhouse)  
 * [Working with Locker](https://github.com/summer-solutions/orm#working-with-locker) 
 * [Working with RabbitMQ](https://github.com/summer-solutions/orm#working-with-rabbitmq) 
 * [Query logging](https://github.com/summer-solutions/orm#query-logging) 
 * [Logger](https://github.com/summer-solutions/orm#logger) 
 * [DataDog Profiler](https://github.com/summer-solutions/orm#datadog-profiler) 
 * [DataDog APM](https://github.com/summer-solutions/orm#datadog-apm)
 * [Event broker](https://github.com/summer-solutions/orm#event-broker)
 * [Tools](https://github.com/summer-solutions/orm#tools)

## Configuration

First you need to define Registry object and register all connection pools to MySQL, Redis, RabbitMQ and local cache.
Use this object to register queues, and entities. You should create this object once when application
starts.

```go
package main

import "github.com/summer-solutions/orm"

func main() {

    registry := &Registry{}

    /*MySQL */
    registry.RegisterMySQLPool("root:root@tcp(localhost:3306)/database_name")
    //optionally you can define pool name as second argument
    registry.RegisterMySQLPool("root:root@tcp(localhost:3307)/database_name", "second_pool")
    registry.DefaultEncoding("utf8") //optional, default is utf8mb4

    /* Redis */
    registry.RegisterRedis("localhost:6379", 0)
    //optionally you can define pool name as second argument
    registry.RegisterRedis("localhost:6379", 1, "second_pool")

    /* Redis ring */
    registry.RegisterRedisRing([]string{"localhost:6379", "localhost:6380"}, 0)

    /* RabbitMQ */
    registry.RegisterRabbitMQServer("amqp://rabbitmq_user:rabbitmq_password@localhost:5672/")
    registry.RegisterRabbitMQQueue(&RabbitMQQueueConfig{Name: "test_queue"})
    registry.RegisterRabbitMQRouter(&RabbitMQRouterConfig{Name, "test_router"})

    /* Local cache (in memory) */
    registry.RegisterLocalCache(1000) //you need to define cache size
    //optionally you can define pool name as second argument
    registry.RegisterLocalCache(100, "second_pool")

    /* Redis used to handle locks (explained later) */
    registry.RegisterRedis("localhost:6379", 4, "lockers_pool")
    registry.RegisterLocker("default", "lockers_pool")

    /* ElasticSearch */
    registry.RegisterElastic("http://127.0.0.1:9200")
    //optionally you can define pool name as second argument
    registry.RegisterElastic("http://127.0.0.1:9200", "second_pool")
    // you can enable trace log
    registry.RegisterElasticWithTraceLog("http://127.0.0.1:9200", "second_pool")

    /* ClickHouse */
    registry.RegisterClickHouse("http://127.0.0.1:9000")
    //optionally you can define pool name as second argument
    registry.RegisterClickHouse("http://127.0.0.1:9000", "second_pool")
}

```

##### You can also create registry using yaml configuration file:

```.yaml
default:
    mysql: root:root@tcp(localhost:3310)/db
    mysqlEncoding: utf8 //optional, default is utf8mb4
    redis: localhost:6379:0
    streams:
      stream-1:
        - test-group-1
        - test-group-2
    elastic: http://127.0.0.1:9200
    elastic_trace: http://127.0.0.1:9201 //with trace log
    clickhouse: http://127.0.0.1:9000
    locker: default
    local_cache: 1000
    rabbitmq:
        server: amqp://rabbitmq_user:rabbitmq_password@localhost:5672/
        queues:
            - name: test
            - name: test2
              durrable: false // optional, default true
              autodelete: false // optional, default false
              prefetchCount: 1 // optional, default 1
              router: test_router // optional, default ""
              ttl: 60 //optional, as seconds, defalut 0 - no TTL  
              router_keys: // optional, default []string
                - aa
                - bb
        routers:
            - name: test_router
              type: direct  
              durable: false // optional, default true
second_pool:
    mysql: root:root@tcp(localhost:3311)/db2
    redis:  // redis ring
        - localhost:6380:0
        - localhost:6381
        - localhost:6382
```

```go
package main

import (
    "github.com/summer-solutions/orm"
    "gopkg.in/yaml.v2"
    "io/ioutil"
)

func main() {

    yamlFileData, err := ioutil.ReadFile("./yaml")
    if err != nil {
        //...
    }
    
    var parsedYaml map[string]interface{}
    err = yaml.Unmarshal(yamlFileData, &parsedYaml)
    registry := InitByYaml(parsedYaml)
}

```

## Defining entities

```go
package main

import (
	"github.com/summer-solutions/orm"
	"time"
)

func main() {

    type AddressSchema struct {
        Street   string
        Building uint16
    }
    
    type colors struct {
        Red    string
        Green  string
        Blue   string
        Yellow string
        Purple string
    }
    var Colors = &colors{
        orm.EnumModel,
    	Red:    "Red",
    	Green:  "Green",
    	Blue:   "Blue",
    	Yellow: "Yellow",
    	Purple: "Purple",
    }

    type testEntitySchema struct {
        orm.ORM
        ID                   uint
        Name                 string `orm:"length=100;index=FirstIndex"`
        NameNullable         string `orm:"length=100;index=FirstIndex"`
        BigName              string `orm:"length=max;required"`
        Uint8                uint8  `orm:"unique=SecondIndex:2,ThirdIndex"`
        Uint24               uint32 `orm:"mediumint=true"`
        Uint32               uint32
        Uint32Nullable       *uint32
        Uint64               uint64 `orm:"unique=SecondIndex"`
        Int8                 int8
        Int16                int16
        Int32                int32
        Int64                int64
        Rune                 rune
        Int                  int
        IntNullable          *int
        Bool                 bool
        BoolNullable         *bool
        Float32              float32
        Float64              float64
        Float64Nullable      *float64
        Float32Decimal       float32  `orm:"decimal=8,2"`
        Float64DecimalSigned float64  `orm:"decimal=8,2;unsigned=false"`
        Enum                 string   `orm:"enum=orm.colorEnum"`
        EnumNotNull          string   `orm:"enum=orm.colorEnum;required"`
        Set                  []string `orm:"set=orm.colorEnum"`
        YearNullable         *uint16   `orm:"year=true"`
        YearNotNull          uint16   `orm:"year=true"`
        Date                 *time.Time
        DateNotNull          time.Time
        DateTime             *time.Time `orm:"time=true"`
        DateTimeNotNull      time.Time  `orm:"time=true"`
        Address              AddressSchema
        Json                 interface{}
        ReferenceOne         *testEntitySchemaRef
        ReferenceOneCascade  *testEntitySchemaRef `orm:"cascade"`
        ReferenceMany        []*testEntitySchemaRef
        IgnoreField          []time.Time       `orm:"ignore"`
        Blob                 []byte
        MediumBlob           []byte `orm:"mediumblob=true"`
        LongBlob             []byte `orm:"longblob=true"`
        FieldAsJson          map[string]string
    }
    
    type testEntitySchemaRef struct {
        orm.ORM
        ID   uint
        Name string
    }
    type testEntitySecondPool struct {
    	orm.ORM `orm:"mysql=second_pool"`
    	ID                   uint
    }

    registry := &Registry{}
    var testEntitySchema testEntitySchema
    var testEntitySchemaRef testEntitySchemaRef
    var testEntitySecondPool testEntitySecondPool
    registry.RegisterEntity(testEntitySchema, testEntitySchemaRef, testEntitySecondPool)
    registry.RegisterEnumStruct("color", Colors)

    // now u can use:
    Colors.GetDefault() // "Red" (first field)
    Colors.GetFields() // ["Red", "Blue" ...]
    Colors.GetMapping() // map[string]string{"Red": "Red", "Blue": "Blue"}
    Colors.Has("Red") //true
    Colors.Has("Orange") //false
    
    //or register enum from slice
    registry.RegisterEnumSlice("color", []string{"Red", "Blue"})
    validatedRegistry.GetEnum("color").GetFields()
    validatedRegistry.GetEnum("color").Has("Red")
    
    //or register enum from map
    registry.RegisterEnumMap("color", map[string]string{"red": "Red", "blue": "Blue"}, "red")
}
```

There are only two golden rules you need to remember defining entity struct: 

 * first field must be type of "ORM"
 * second argument must have name "ID" and must be type of one of uint, uint16, uint32, uint24, uint64, rune
 
 
 By default entity is not cached in local cache or redis, to change that simply use key "redisCache" or "localCache"
 in "orm" tag for "ORM" field:
 
 ```go
 package main
 
 import (
 	"github.com/summer-solutions/orm"
 	"time"
 )
 
 func main() {
 
     type testEntityLocalCache struct {
     	orm.ORM `orm:"localCache"` //default pool
        //...
     }
    
    type testEntityLocalCacheSecondPool struct {
     	orm.ORM `orm:"localCache=second_pool"`
        //...
     }
    
    type testEntityRedisCache struct {
     	orm.ORM `orm:"redisCache"` //default pool
        //...
     }
    
    type testEntityRedisCacheSecondPool struct {
     	orm.ORM `orm:"redisCache=second_pool"`
        //...
     }

    type testEntityLocalAndRedisCache struct {
     	orm.ORM `orm:"localCache;redisCache"`
        //...
     }
 }
 ```

## Validated registry

Once you created your registry and registered all pools and entities you should validate it.
You should also run it once when your application starts.

 ```go
 package main
 
 import "github.com/summer-solutions/orm"
 
 func main() {
    registry := &Registry{}
    //register pools and entities
    validatedRegistry, err := registry.Validate()
 }
 
 ```
 
 ## Creating engine
 
 You need to crete engine to start working with entities (searching, saving).
 You must create engine for each http request and thread.
 
  ```go
  package main
  
  import "github.com/summer-solutions/orm"
  
  func main() {
     registry := &Registry{}
     //register pools and entities
     validatedRegistry, err := registry.Validate()
     engine := validatedRegistry.CreateEngine()
  }
  
  ```
 
 ## Checking and updating table schema
 
 ORM provides useful object that describes entity structrure called TabelSchema:
 
 ```go
 package main
 
 import "github.com/summer-solutions/orm"
 
 func main() {
    
    registry := &Registry{}
    // register
    validatedRegistry, err := registry.Validate() 
    engine := validatatedRegistry.CreateEngine()
    alters := engine.GetAlters()
    
    /*optionally you can execute alters for each model*/
    var userEntity UserEntity
    tableSchema := engine.GetRegistry().GetTableSchemaForEntity(userEntity)
    //or
    tableSchema := validatedRegistry.GetTableSchemaForEntity(userEntity)

    /*checking table structure*/
    tableSchema.UpdateSchema(engine) //it will create or alter table if needed
    tableSchema.DropTable(engine) //it will drop table if exist
    tableSchema.TruncateTable(engine)
    tableSchema.UpdateSchemaAndTruncateTable(engine)
    has, alters := tableSchema.GetSchemaChanges(engine)

    /*getting table structure*/
    db := tableSchema.GetMysql(engine)
    localCache, has := tableSchema.GetLocalCache(engine) 
    redisCache, has := tableSchema.GetRedisCache(engine)
    columns := tableSchema.GetColumns()
    tableSchema.GetTableName()
 }
 
 ```


## Adding, editing, deleting entities

```go
package main

import "github.com/summer-solutions/orm"

func main() {

     /* adding */

    entity := testEntity{Name: "Name 1"}
    engine.TrackAndFlush(&entity)

    entity2 := testEntity{Name: "Name 1"}
    engine.SetOnDuplicateKeyUpdate(NewWhere("`counter` = `counter` + 1"), entity2)
    engine.TrackAndFlush(&entity)

    entity2 = testEntity{Name: "Name 1"}
    engine.SetOnDuplicateKeyUpdate(NewWhere(""), entity2) //it will change nothing un row
    engine.TrackAndFlush(&entity)

    /*if you need to add more than one entity*/
    entity = testEntity{Name: "Name 2"}
    entity2 := testEntity{Name: "Name 3"}
    engine.Track(&entity, &entity2) //it will also automatically run RegisterEntity()
    //it will execute only one query in MySQL adding two rows at once (atomic)
    engine.Flush()
 
    /* editing */

    engine.Track(&entity, &entity2)
    entity.Name = "New name 2"
    //you can also use (but it's slower):
    entity.SetField("Name", "New name 2")
    engine.IsDirty(entity) //returns true
    engine.IsDirty(entity2) //returns false
    entity.Flush() //it will save data in DB for all dirty tracked entities and untrack all of them
    engine.IsDirty(entity) //returns false
    
    /* deleting */
    engine.MarkToDelete(entity2)
    engine.IsDirty(entity2) //returns true
    engine.Flush()

    /* flush will panic if there is any error. You can catch 2 special errors using this method  */
    err := engine.FlushWithCheck()
    //or
    err := engine.FlushInTransactionWithCheck()
    orm.DuplicatedKeyError{} //when unique index is broken
    orm.ForeignKeyError{} //when foreign key is broken
    
    /* You can catch all errors using this method  */
    err := engine.FlushWithFullCheck()
}
```

## Transactions

```go
package main

import "github.com/summer-solutions/orm"

func main() {
	
    entity = testEntity{Name: "Name 2"}
    entity2 := testEntity{Name: "Name 3"}
    engine.Track(&entity, &entity2)

    // DB transcation
    engine.FlushInTransaction()
    // or redis lock
    engine.FlushWithLock("default", "lock_name", 10 * time.Second, 10 * time.Second)
    // or DB transcation nad redis lock
    engine.FlushInTransactionWithLock("default", "lock_name", 10 * time.Second, 10 * time.Second)
 
    //manual transaction
    db := engine.GetMysql()
    db.Begin()
    defer db.Rollback()
    //run queries
    db.Commit()
```

## Loading entities using primary key

```go
package main

import "github.com/summer-solutions/orm"

func main() {

    var entity testEntity
    has := engine.LoadByID(1, &entity)

    var entities []*testEntity
    missing := engine.LoadByIDs([]uint64{1, 3, 4}, &entities) //missing contains IDs that are missing in database

}

```

## Loading entities using search

```go
package main

import "github.com/summer-solutions/orm"

func main() {

    var entities []*testEntity
    pager := orm.NewPager(1, 1000)
    where := orm.NewWhere("`ID` > ? AND `ID` < ?", 1, 8)
    engine.Search(where, pager, &entities)
    
    //or if you need number of total rows
    totalRows := engine.SearchWithCount(where, pager, &entities)
    
    //or if you need only one row
    where := onm.NewWhere("`Name` = ?", "Hello")
    var entity testEntity
    found := engine.SearchOne(where, &entity)
    
    //or if you need only primary keys
    ids := engine.SearchIDs(where, pager, entity)
    
    //or if you need only primary keys and total rows
    ids, totalRows = engine.SearchIDsWithCount(where, pager, entity)
}

```

## Reference one to one

```go
package main

import "github.com/summer-solutions/orm"

func main() {

    type UserEntity struct {
        ORM
        ID                   uint64
        Name                 string
        School               *SchoolEntity `orm:"required"` // key is "on delete restrict" by default not not nullable
        SecondarySchool      *SchoolEntity // key is nullable
    }
    
    type SchoolEntity struct {
        ORM
        ID                   uint64
        Name                 string
    }

    type UserHouse struct {
        ORM
        ID                   uint64
        User                 *UserEntity  `orm:"cascade;required"` // on delete cascade and is not nullable
    }
    
    // saving in DB:

    user := UserEntity{Name: "John"}
    school := SchoolEntity{Name: "Name of school"}
    house := UserHouse{Name: "Name of school"}
    engine.Track(&user, &school, &house)
    user.School = school
    house.User = user
    engine.Flush()

    // loading references: 

    _ = engine.LoadById(1, &user)
    user.School != nil //returns true, School has ID: 1 but other fields are nof filled
    user.School.ID == 1 //true
    user.School.Loaded() //false
    user.Name == "" //true
    user.School.Load(engine) //it will load school from db
    user.School.Loaded() //now it's true, you can access school fields like user.School.Name
    user.Name == "Name of school" //true
    
    //If you want to set reference and you have only ID:
    user.School = &SchoolEntity{ID: 1}

    // detaching reference
    user.School = nil

    // preloading references
    engine.LoadByID(1, &user, "*") //all references
    engine.LoadByID(1, &user, "School") //only School
    engine.LoadByID(1, &user, "School", "SecondarySchool") //only School and SecondarySchool
    engine.LoadByID(1, &userHouse, "User/School", "User/SecondarySchool") //User, School and SecondarySchool in each User
    engine.LoadByID(1, &userHouse, "User/*") // User, all references in User
    engine.LoadByID(1, &userHouse, "User/*/*") // User, all references in User and all references in User subreferences
    //You can have as many levels you want: User/School/AnotherReference/EvenMore/
    
    //You can preload referenes in all search and load methods:
    engine.LoadByIDs()
    engine.Search()
    engine.SearchOne()
    engine.CachedSearch()
    ...
}

```

## Cached queries

```go
package main

import "github.com/summer-solutions/orm"

func main() {

    //Fields that needs to be tracked for changes should start with ":"

    type UserEntity struct {
        ORM
        ID                   uint64
        Name                 string
        Age                  uint16
        IndexAge             *CachedQuery `query:":Age = ? ORDER BY :ID"`
        IndexAll             *CachedQuery `query:""` //cache all rows
        IndexName            *CachedQuery `queryOne:":Name = ?"`
    }

    pager := orm.NewPager(1, 1000)
    var users []*UserEntity
    var user  UserEntity
    totalRows := engine.CachedSearch(&users, "IndexAge", pager, 18)
    totalRows = engine.CachedSearch(&users, "IndexAll", pager)
    has := engine.CachedSearchOne(&user, "IndexName", "John")

}

```

## Lazy flush

Sometimes you want to flush changes in database, but it's ok if data is flushed after some time. 
For example when you want to save some logs in database.

```go
package main

import "github.com/summer-solutions/orm"

func main() {
    
    // you need to register redis  
    registry.RegisterRedis("localhost:6379", 0)
    registry.RegisterRedis("localhost:6380", 0, "another_redis")
    
    // .. create engine

    type User struct {
       ORM  `orm:"log"`
       ID   uint
       Name string
       Age  int `orm:"skip-log"` //Don't track this field
    }
   
    // optionally you can set optional redis pool used to queue all events
    type Dog struct {
       ORM  `orm:"asyncRedisLazyFlush=another_redis"`
       ID   uint
       Name string
    }
       
    // now in code you can use FlushLazy() methods instead of Flush().
    // it will send changes to queue (database and cached is not updated yet)
    engine.FlushLazy()
    
    // you need to run code that will read data from queue and execute changes
    // run in separate goroutine (cron script)
    consumer := NewAsyncConsumer(engine, "my-consumer", 1) // you can run maximum one consumer
    consumer.Digest() //It will wait for new messages in a loop, run receiver.DisableLoop() to run loop once

    consumerAnotherPool := NewAsyncConsumer(engine,  "my-consumer", 5) // you can run up to 5 consumers at the same time
    consumerAnotherPool.Digets()
}

```

## Request cache

It's a good practice to cache entities in one short request (e.g. http request) to reduce number of requests to databases.

If you are using more than one goroutine (for example in GraphQL backend implementation) you can enable Data loader in engine to group many queries into one and reduce
number of queries. You can read more about idea behind it [here](https://gqlgen.com/reference/dataloaders/).

```go
package main

import "github.com/summer-solutions/orm"

func main() {
    engine.EnableDataLoader(true)
}
```

Otherwise set false and all entities will be cached in a simple temporary cache:

```go
package main

import "github.com/summer-solutions/orm"

func main() {
    engine.EnableDataLoader(false) 
}

```

## Log entity changes

ORM can store in database every change of entity in special log table.

```go
package main

import "github.com/summer-solutions/orm"

func main() {

    //it's recommended to keep logs in separated DB
    registry.RegisterMySQLPool("root:root@tcp(localhost:3306)/log_database", "log_db_pool")
    // you need to register default Redis   
    registry.RegisterRedis("localhost:6379", 0)
    registry.RegisterRedis("localhost:6380", 0, "another_redis")

    //next you need to define in Entity that you want to log changes. Just add "log" tag or define mysql pool name
    type User struct {
        ORM  `orm:"log"`
        ID   uint
        Name string
        Age  int `orm:"skip-log"` //Don't track this field
    }
    
    // optionally you can set optional redis pool used to queue all events
     type Dog struct {
        ORM  `orm:"log=log_db_pool;asyncRedisLogs=another_redis"`
        ID   uint
        Name string
     }

    // Now every change of User will be saved in log table
    
    // You can add extra data to log, simply use this methods before Flush():
    engine.SetLogMetaData("logged_user_id", 12) 
    engine.SetLogMetaData("ip", request.GetUserIP())
    // you can set meta only in specific entity
    engine.SetEntityLogMeta("user_name", "john", entity)
    
    consumer := NewAsyncConsumer(engine, "my-consumer",  1)
    consumer.Digets() //it will wait for new messages in queue

    consumerAnotherPool := NewAsyncConsumer(engine, "my-consumer", 1)
    consumerAnotherPool.Digets()
}

```

## Dirty stream

You can send event to event broker if any specific data in entity was changed.

```go
package main

import "github.com/summer-solutions/orm"

func main() {

	//define at least one redis pool
    registry.RegisterRedis("localhost:6379", 0, "event-broker-pool")
    //define stream with consumer groups for events
    registry.RegisterRedisStream("user_changed", "event-broker-pool", []string{"my-consumer-group"})
    registry.RegisterRedisStream("age_name_changed", "event-broker-pool", []string{"my-consumer-group"})
    registry.RegisterRedisStream("age_changed", "event-broker-pool", []string{"my-consumer-group"})

    // create engine
    
    // next you need to define in Entity that you want to log changes. Just add "dirty" tag
    type User struct {
        orm.ORM  `orm:"dirty=user_changed"` //define dirty here to track all changes
        ID       uint
        Name     string `orm:"dirty=age_name_changed"` //event will be send to age_name_changed if Name or Age changed
        Age      int `orm:"dirty=age_name_changed,age_changed"` //event will be send to age_changed if Age changed
    }
 
    consumer := engine.GetEventBroker().Consume("my-consumer", "my-consumer-group", 1)

    consumer.Consume(context.Background(), 100, func(events []orm.Event) {
        for _, event := range events {
           dirty := orm.EventDirtyEntity(event) // created wrapper around event to easily access data
           if dirty.Added() {
           	 fmt.Printf("Entity %s with ID %d was added", dirty.TableSchema().GetType().String(), dirty.ID())
           } else if dirty.Updated() {
           	 fmt.Printf("Entity %s with ID %d was updated", dirty.TableSchema().GetType().String(), dirty.ID())
           } else if dirty.Deleted() {
             fmt.Printf("Entity %s with ID %d was deleted", dirty.TableSchema().GetType().String(), dirty.ID())
           }
           event.Ack()
        }
    })
}


```

## Fake delete

If you want to keep deleted entity in database but ny default this entity should be excluded
from all engine.Search() and engine.CacheSearch() queries you can use FakeDelete column. Simply create
field bool with name "FakeDelete".

```go
func main() {

    type UserEntity struct {
        ORM
        ID                   uint64
        Name                 string
        FakeDelete           bool
    }

    //you can delete in two ways:
    engine.MarkToDelete(user) -> will set user.FakeDelete = true
    //or:
    user.FakeDelete = true

    engine.Flush(user) //it will save entity id in Column `FakeDelete`.

    //will return all rows where `FakeDelete` = 0
    total, err = engine.SearchWithCount(NewWhere("1"), nil, &rows)

    //To force delete (remove row from DB):
    engine.ForceMarkToDelete(user)
    engine.Flush(user)
}


```

## Working with Redis

```go
package main

import "github.com/summer-solutions/orm"

func main() {

    config.RegisterRedis("localhost:6379", 0)
    
    //storing data in cached for x seconds
    val := engine.GetRedis().GetSet("key", 1, func() interface{} {
		return "hello"
	})
    
    //standard redis api
    keys := engine.GetRedis().LRange("key", 1, 2)
    engine.GetRedis().LPush("key", "a", "b")
    //...

    //rete limiter
    valid := engine.GetRedis().RateLimit("resource_name", redis_rate.PerMinute(10))
}

```


## Working with local cache

```go
package main

import "github.com/summer-solutions/orm"

func main() {
    
    registry.RegisterLocalCache(1000)
    
    //storing data in cached for x seconds
    val := engine.GetLocalCache().GetSet("key", 1, func() interface{} {
        return "hello"
    })
    
    //getting value
    value, has := engine.GetLocalCache().Get("key")
    
    //getting many values
    values := engine.GetLocalCache().MGet("key1", "key2")
    
    //setting value
    engine.GetLocalCache().Set("key", "value")
    
    //setting values
    engine.GetLocalCache().MSet("key1", "value1", "key2", "value2" /*...*/)
    
    //getting values from hash set (like redis HMGET)
    values = engine.GetLocalCache().HMget("key")
    
    //setting values in hash set
    engine.GetLocalCache().HMset("key", map[string]interface{}{"key1" : "value1", "key2": "value 2"})

    //deleting value
    engine.GetLocalCache().Remove("key1", "key2" /*...*/)
    
    //clearing cache
    engine.GetLocalCache().Clear()

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
    
    // register mysql pool
    registry.RegisterMySQLPool("root:root@tcp(localhost:3306)/database_name")

    res := engine.GetMysql().Exec("UPDATE `table_name` SET `Name` = ? WHERE `ID` = ?", "Hello", 2)

    var row string
    found := engine.GetMysql().QueryRow(orm.NewWhere("SELECT * FROM `table_name` WHERE  `ID` = ?", 1), &row)
    
    results, def := engine.GetMysql().Query("SELECT * FROM `table_name` WHERE  `ID` > ? LIMIT 100", 1)
    defer def()
    for results.Next() {
    	var row string
        results.Scan(&row)
    }
    def() //if it's not last line in this method
}

```

## Working with elastic search

```go
package main

import (
    "github.com/summer-solutions/orm"
)

func main() {

    type TestIndex struct {
    }
    
    func (i *TestIndex) GetName() string {
    	return "test_index"
    }
    
    func (i *TestIndex) GetDefinition() map[string]interface{} {
        return map[string]interface{}{
            "settings": map[string]interface{}{
                "number_of_replicas": "1",
                "number_of_shards":   "1",
            },
            "mappings": map[string]interface{}{
                "properties": map[string]interface{}{
                    "Name": map[string]interface{}{
                        "type":       "keyword",
                        "normalizer": "case_insensitive",
                    },
                },
            },
        }
    }

    
    // register elastic search pool and index
    registry.RegisterElastic("http://127.0.0.1:9200")
    registry.RegisterElasticIndex(&TestIndex{})


    e := engine.GetElastic()

    // create indices
	for _, alter := range engine.GetElasticIndexAlters() {
        // alter.Safe is true if index does not exists or is not empty
		engine.GetElastic(alter.Pool).CreateIndex(alter.Index)
	}

    query := elastic.NewBoolQuery()
	query.Must(elastic.NewTermQuery("user_id", 12))
    options := &orm.SearchOptions{}
    options.AddSort("created_at", true).AddSort("name", false)
	results := e.Search("users", query, orm.NewPager(1, 10), options)
}

```

## Working with ClickHouse

```go
package main

import (
    "github.com/summer-solutions/orm"
)

func main() {
    
    // register elastic search pool
    registry.RegisterClickHouse("http://127.0.0.1:9000")

    ch := engine.GetClickHouse()

    ch.Exec("INSERT INTO `table` (name) VALUES (?)", "hello")

    statement, def := ch.Prepare("INSERT INTO `table` (name) VALUES (?)")
    defer def()
    statement.Exec("hello")
    statement.Exec("hello 2")

    rows, def := ch.Queryx("SELECT FROM `table` WHERE x = ? AND y = ?", 1, "john")
    defer def()
    for rows.Next() {
    	m := &MyStruct{}
        err := rows.StructScan(m)
    }

    ch.Begin()
    defer ch.Rollback()
    // run queries
    defer ch.Commit()
}

```

## Working with Locker

Shared cached that is using redis

```go
package main

import "github.com/summer-solutions/orm"

func main() {

    // register redis and locker
    registry.RegisterRedis("localhost:6379", 0, "my_pool")
    registry.RegisterLocker("default", "my_pool")
    
    locker, _ := engine.GetLocker()
    lock := locker.Obtain("my_lock", 5 * Time.Second, 1 * Time.Second)

    defer lock.Release()
    
    // do smth
    
    ttl := lock.TTL()
    if ttl == 0 {
        panic("lock lost")
    }
}

```

## Working with RabbitMQ

```go
package main

import "github.com/summer-solutions/orm"

func main() {

    // register rabbitMQ servers, queues and routers
    registry.RegisterRabbitMQServer("amqp://rabbitmq_user:rabbitmq_password@localhost:5672/")
    registry.RegisterRabbitMQQueue(&RabbitMQQueueConfig{Name: "test_queue", TTL: 60}) //ttl set to 60 seconds
    registry.RegisterRabbitMQQueue(&RabbitMQQueueConfig{Name: "test_queue_router", 
        Router: "test_router", RouteKeys: []string{"aa", "bb"}})
    registry.RegisterRabbitMQRoutere("default", &RabbitMQRouteConfig{Name: "test_router", Type: "fanout"})
    
    //create engine:
    validatedRegistry, err := registry.Validate()
    engine := validatedRegistry.CreateEngine()
    defer engine.Defer() //it will close all opened channels

    //Simple queue
    channel := engine.GetRabbitMQQueue("test_queue") //provide Queue name
    defer channel.Close()
    channel.GetEventBroker().PublishMap([]byte("hello"))

    //start consumer (you can add as many you want)
    consumer, err := channel.NewConsumer("test consumer")
    defer consumer.Close()
    consumer.Consume(func(items [][]byte) {
    	//do staff
    })

    //start consumer (you can add as many you want)
    consumer := channel.NewConsumer("test consumer")
    defer consumer.Close()
    consumer.Consume(func(items [][]byte) {
        //do staff
    })
    
    // publish to router

    channel = engine.GetRabbitMQRouter("test_queue_router") 
    defer channel.Close()
    channel.GetEventBroker().PublishMap("router.key", []byte("hello"))

    //start consumer
   consumer := channel.NewConsumer("test consumer")
   defer consumer.Close()
   consumer.Consume(func(items [][]byte) {
        //do staff
        return nil
   })
}

```


## Query logging

You can log all queries:
 
 * queries to MySQL database (insert, select, update)
 * requests to Redis
 * requests to rabbitMQ 
 * requests to Elastic Search 
 * queries to CickHouse 

```go
package main

import "github.com/summer-solutions/orm"

func main() {
	
    //enable human friendly console log
    engine.EnableQueryDebug() //MySQL, redis, rabbitMQ, Elastic Search, ClickHouse queries (local cache in excluded bt default)
    engine.EnableQueryDebug(orm.QueryLoggerSourceRedis, orm.QueryLoggerSourceLocalCache)

    //adding custom logger example:
    engine.AddQueryLogger(json.New(os.Stdout), log.LevelWarn) //MySQL, redis, rabbitMQ warnings and above
    engine.AddQueryLogger(es.New(os.Stdout), log.LevelError, orm.QueryLoggerSourceRedis, orm. QueryLoggerSourceRabbitMQ)
}    
```

## Logger

```go
package main

import "github.com/summer-solutions/orm"

func main() {
	
    //enable json logger with proper level
    engine.EnableLogger(log.InfoLevel)
    //or enable human friendly console logger
    engine.EnableDebug()
    
    //you can add special fields to all logs
    engine.Log().AddFields(log.Fields{"user_id": 12, "session_id": "skfjhfhhs1221"})

    //printing logs
    engine.Log().Warn("message", nil)
    engine.Log().Debug("message", log.Fields{"user_id": 12})
    engine.Log().Error(err, nil)
    engine.Log().ErrorMessage("ups, that is strange", nil)


    //handling recovery
    if err := recover(); err != nil {
    	engine.Log().Error(err, nil)
    }

    //filling log with data from http.Request
    engine.Log().AddFieldsFromHTTPRequest(request, "197.21.34.22")

}    
```

## DataDog Profiler

To enable DataDog profiler simply add two lines of code in your main function.
Provide your service name, datadog API key, environment name (production, test, ..n) and
interval how often system should send profiler data to Datadog 

```go
package main

import "github.com/summer-solutions/orm"

func main() {
	
    def := orm.StartDataDogProfiler("my-app-name", "DATADOG-API-KEY", "production", time.Minute)
    defer def()

}    
```

## DataDog APM

First you need to register it in your main function

```go
package main

import "github.com/summer-solutions/orm"

func main() {
    
   //provide rate, 1 - 100% traces are reported, 0.1 - 10% traces are reported (and all with errors)
   // if you provide zero only traces with errors will be reported
   def := orm.StartDataDogTracer(1.0) 
   defer def()

}    
```

Start trace for HTTP request. Example in Gin framework:

```go
package main

import "github.com/summer-solutions/orm"

func main() {
    
    router := gin.New()
    //you should define it as first middleware
    router.Use(func(c *gin.Context) {
        engine := // create orm.Engine
        apm := engine.DataDog().StartHTTPAPM(c.Request, "my-app-name", "production")
        defer apm.Finish()
    
        //optionally enable ORM APM services
        engine.DataDog().EnableORMAPMLog(log.InfoLevel, true) //log ORM requests (MySQl, RabbitMQ, Redis queries) as services

        c.Next()
        apm.SetResponseStatus(c.Writer.Status())
    })

}    
```

Start trace in scripts (for example in cron scripts):

```go
package main

import "github.com/summer-solutions/orm"

func main() {
    
    engine.DataDog().StartAPM("my-script-name", "production")
    defer engine.DataDog().FinishAPM()
    //optionally enable ORM APM services
    engine.DataDog().EnableORMAPMLog(log.InfoLevel, true)
    //execute your code
}    
```

Start trace in intermediate scripts:

```go
package main

import "github.com/summer-solutions/orm"

func main() {
    
    engine := //
    engine.DataDog().StartAPM("my-script-name", "production")
    defer engine.DataDog().FinishAPM()
    engine.DataDog().EnableORMAPMLog(log.InfoLevel, true)

    heartBeat := func() {
        engine.DataDog().FinishAPM()
        engine.DataDog().StartAPM("my-script-name", "production")
    }
    consumer := orm.NewAsyncConsumer(engine, "test-consumer", 1)
    consumer.SetHeartBeat(heartBeat) //consumer will execute this method every minute
    consumer.Digest(ctx.Background())

}    
```

You should always assign unexpected error to APM trace

```go
package main

import "github.com/summer-solutions/orm"

func main() {
    
    if r := recover(); r != nil {
        engine.DataDog().RegisterAPMError(r)
    }
}    
```

Extra operations:

```go
package main

import "github.com/summer-solutions/orm"

func main() {

	engine.DataDog().DropAPM() //it will drop trace. Only traces with errors will be recorded
    
    engine.DataDog().SetAPMTag("user_id", 12)
    
    //sub tasks
    func() {
    	span := engine.DataDog().StartWorkSpan("logging user")
        span.setTag("user_name", "Tom")
        defer.span.Finish()
        //some work
    }()

}    
```

## Event broker

ORM provides easy way to use event broker.

First yuo need to define streams and consumer groups:

```yaml
#YAML config file
default:
  redis: localhost:6381:0 // redis is required
  streams:
    stream-1:
      - test-group-1
      - test-group-2
    stream-2:
      - test-group-1
    stream-3:
      - test-group-3
```
or using go:

```go
package main

import "github.com/summer-solutions/orm"

func main() {
 registry := &orm.Registry{}
 registry.RegisterRedisStream("stream-1", "default", []string{"test-group-1", "test-group-2"})
 registry.RegisterRedisStream("stream-2", "default", []string{"test-group-1"})
 registry.RegisterRedisStream("stream-3", "default", []string{"test-group-3"})
}    
```


Publishing and receiving eventsr:

```go
package main

import (
 "context"
 "github.com/summer-solutions/orm"
)

func main() {

 // .. create engine

 type Person struct {
  Name string
  Age  int
 }

 // fastest, no serialization
 engine.GetEventBroker().PublishMap("stream-1", orm.EventAsMap{"key": "value", "anotherKey": "value 2"})
 // using serialization
 engine.GetEventBroker().Publish("stream-3", Person{Name: "Adam", Age: 18})

 // reading from "stream-1" and "stream-2" streams, you can run max one consumer at once
 consumerTestGroup1 := engine.GetEventBroker().Consume("my-consumer", "test-group-1", 1)
 
 // reading max 100 events in one loop, this line stop execution, waiting for new events
 consumerTestGroup1.Consume(context.Background(), 100, func(events []orm.Event) {
 	for _, event := range events {
            values := event.RawData() // map[string]interface{}{"key": "value", "anotherKey": "value 2"}
            //do some work
            event.Ack() // this line is acknowledging event
 	}
 })

 // auto acknowledging
 consumerTestGroup1.Consume(context.Background(), 100, func(events []orm.Event) { 
 	//do some work, for example put all events at once to DB
 	// in this example all events will be acknowledge when this method is finished 
 })

 // skipping some events
 consumerTestGroup1.Consume(context.Background(), 100, func(events []orm.Event) {
  for _, event := range events {
        if someCondition {
             event.Ack()
        } else {
             event.Skip() //this event will be consumed again later
        }
    }
 })

 // reading from "stream-3" stream, you can run max to two consumers at once
 consumerTestGroup3 := engine.GetEventBroker().Consume("my-consumer", "test-group-2", 2)
 consumerTestGroup3.DisableLoop() // all events will be consumed once withour waiting for new events   

 consumerTestGroup3.Consume(context.Background(), 100, func(events []orm.Event) {
    var person Person
 	for _, event := range events {
        err := event.Unserialize(&person)
        if err != nil {
        	// ...
        }
        //do some work
        event.Ack() // this line is acknowledging event
    }
 })

}    
```

Setting another redis pool for AsyncConsumer:
```yaml
another_pool:
  redis: localhost:6381:0
  streams:
   orm-lazy-channel: # FlushLazy()
      - default-group # group name for AsyncConsumer
    orm-log-channel: # adding changes to logs
      - default-group # group name for AsyncConsumer
      - test-group-2 # you can register another consumers to read logs  
```


## Tools

Redis streams statistics

```go
package main

import "github.com/summer-solutions/orm/tools"

func main() {
   stats := tools.GetRedisStreamsStatistics(engine) 
}    
```