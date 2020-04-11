# orm

## Defining database and cache pool connections

First you need to define config. You should do it once, 
when your application starts.

```go
package main

import "github.com/summer-solutions/orm"

func main() {

    registry := &orm.Registry{}

    /*MySQL */
    registry.RegisterMySQLPool("root:root@tcp(localhost:3306)/database_name")
    //optionally you can define pool name as second argument
    registry.RegisterMySQLPool("root:root@tcp(localhost:3307)/database_name", "second_pool")

    /* Redis */
    registry.RegisterRedis("localhost:6379", 0) //seconds argument is a redis database number
    //optionally you can define pool name as second argument
    registry.RegisterRedis("localhost:6379", 1, "second_pool")

    /* Redis used to handle queues (explained later) */
    registry.RegisterRedis("localhost:6379", 3, "queues_pool")
    registry.RegisterLazyQueue("default", "queues_pool")//if not defined orm is using default redis pool

    /* Local cache (in memory) */
    registry.RegisterLocalCache(1000) //you need to define cache size
    //optionally you can define pool name as second argument
    registry.RegisterLocalCache(100, "second_pool")

    /* Redis used to handle locks (explained later) */
    registry.RegisterRedis("localhost:6379", 4, "lockers_pool")
    registry.RegisterLocker("default", "lockers_pool")
    
    config, err := registry.CreateConfig()

}

```

##### You can also create config using yaml configuration file:

```.yaml

orm:
  default:
    mysql: root:root@tcp(localhost:3310)/db
    redis: localhost:6379:0
    redisQueues: localhost:6379:1
    lazyQueue: redisQueues
    locker: default
    dirtyQueue: redisQueues
    localCache: 1000
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
    config, err := orm.InitByYaml(parsedYaml)
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
        Orm                  *orm.ORM
        ID                   uint
        Name                 string `orm:"length=100;index=FirstIndex"`
        NameNotNull          string `orm:"length=100;index=FirstIndex;required"`
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
        Enum                 string   `orm:"enum=tests.Color"`
        EnumNotNull          string   `orm:"enum=tests.Color;required"`
        Set                  []string `orm:"set=tests.Color"`
        Year                 uint16   `orm:"year=true"`
        YearNotNull          uint16   `orm:"year=true;required"`
        Date                 time.Time
        DateNotNull          time.Time `orm:"required"`
        DateTime             time.Time `orm:"time=true"`
        Address              AddressSchema
        Json                 interface{}
        ReferenceOne         *orm.ReferenceOne `orm:"ref=tests.TestEntitySchemaRef"`
        ReferenceOneCascade  *orm.ReferenceOne `orm:"ref=tests.TestEntitySchemaRef;cascade"`
        IgnoreField          []time.Time       `orm:"ignore"`
        Blob                 []byte
    }
    
    type TestEntitySchemaRef struct {
        Orm  *orm.ORM
        ID   uint
        Name string
    }
    type TestEntitySecondPool struct {
    	Orm                  *orm.ORM `orm:"mysql=second_pool"`
    	ID                   uint
    }

    registry := &orm.Registry{}
    registry.RegisterEntity(TestEntitySchema{}, TestEntitySchemaRef{}, TestEntitySecondPool{})
    registry.RegisterEnum("Color", Color)

}
```

There are only two golden rules you need to remember defining entity struct: 

 * first field must have name "Orm" and must be type of "*orm.ORM"
 * second argument must have name "ID" or "ID" and must be type of one of uint, uint16, uint32, uint64
 
 
 As you can see orm is not using null values like sql.NullString. Simply set empty string "" and orm will
 convert it to null in database if needed. 
 
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
 
     orm.RegisterMySQLPool("root:root@tcp(localhost:3306)/database_name")
    
     type FirstEntity struct {
        Orm                  *orm.ORM
        ID                   uint
        Name                 string
     }
      
    type SecondEntity struct {
        Orm                  *orm.ORM
        ID                   uint
        Name                 string
    }
    
    var firstEntity  FirstEntity
    var secondEntity SecondEntity
    registry := &orm.Registry{}
	registry.RegisterEntity(firstEntity, secondEntity)
    config, err := registry.CreateConfig()
    
    engine := orm.NewEngine(config)
    alters, err := engine.GetAlters()
    
    /*optionally you can execute alters for each model*/
    config.GetTableSchema(firstEntity).UpdateSchema() //it will create or alter table if needed
    config.GetTableSchema(firstEntity).DropTable() //it will drop table if exist
    config.GetTableSchema(firstEntity).TruncateTable()
    //if you need to see queries:
    has, alters, err := orm.GetTableSchema(firstEntity).GetSchemaChanges()
 }
 
 ```


## Adding, editing, deleting entities

```go
package main

import "github.com/summer-solutions/orm"

func main() {

    registry := &orm.Registry{}
    registry.RegisterMySQLPool("root:root@tcp(localhost:3306)/database_name")

    type TestEntity struct {
        Orm                  *orm.ORM
        ID                   uint
        Name                 string
    }
    var entity TestEntity
    registry.RegisterEntity(entity)
    //code above you should execute only once, when application starts
    
    config, err := registry.NewConfig()
    engine := orm.NewEngine(config)

    entity = TestEntity{Name: "Name 1"}
    engine.Init(&entity) // you should use this function only for new entities
    err := engine.Flush(&entity)
    if err != nil {
       ///...
    }

    /*if you need to add more than one entity*/
    entity = TestEntity{Name: "Name 2"}
    entity2 := TestEntity{Name: "Name 3"}
    engine.Init(&entity, &entity2)
    //it will execute only one query in MySQL adding two rows at once (atomic)
    err = engine.Flush(&entity, &entity2)
    if err != nil {
       ///...
    }

    /* editing */
    entity.Name = "New name 2"
    engine.IsDirty(entity) //returns true
    engine.IsDirty(entity2) //returns false
    err = engine.Flush(&entity)
    if err != nil {
       ///...
    }
    engine.IsDirty(entity) //returns false
    
    /* deleting */
    entity2.Orm.MarkToDelete()
    engine.IsDirty(entity2) //returns true
    err = engine.Flush(&entity2)
    if err != nil {
       ///...
    }

    /* flush can return 2 special errors */
    orm.DuplicatedKeyError{} //when unique index is broken
    orm.ForeignKeyError{} //whne foreign key is broken
}

```

If you need to work with more than one entity i strongly recommend ot use FLusher (described later).


## Getting entities using primary keys

```go
package main

import "github.com/summer-solutions/orm"

func main() {

 
    type TestEntity struct {
        Orm                  *orm.ORM
        ID                   uint
        Name                 string
    }
    var entity TestEntity
    registry := &orm.Registry{}
   //.. register pools and entities
    config, err := registry.NewConfig()
    engine := orm.NewEngine(config)

    found, err := engine.TryByID(1, &entity) //found has false if row does not exists
    err = engine.GetByID(2, &entity) //if will return err if entity does not exists

    var entities []*TestEntity
    //missing is []uint64 that contains id of rows that doesn't exists, 
    // in this cause $found slice has nil for such keys
    missing, err := engine.TryByIDs([]uint64{2, 3, 1}, &entities) 
    err = engine.GetByIDs([]uint64{2, 3, 1}, &entities) //will return error if at least one row does not exist
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
        ID                   uint
        Name                 string
    }
    registry := &orm.Registry{}
   //.. register pools and entities
    config, err := registry.CreateConfig()
    engine := orm.NewEngine(config)

    var entities []*TestEntity
    pager := orm.Pager{CurrentPage: 1, PageSize: 100}
    where := orm.NewWhere("`ID` > ? AND `ID` < ?", 1, 8)
    err := engine.Search(where, pager, &entities)
    
    //or if you need number of total rows
    totalRows, err := engine.SearchWithCount(where, pager, &entities)
    
    //or if you need only one row
    where := orm.NewWhere("`Name` = ?", "Hello")
    var entity TestEntity
    found, err := engine.SearchOne(where, &entity)
    
    //or if you need only primary keys
    ids, err := engine.SearchIDs(where, pager, entity)
    
    //or if you need only primary keys and total rows
    ids, totalRows, err = engine.SearchIDsWithCount(where, pager, entity)
}

```

## Flusher and AutoFlusher

Very often you need to change more than one entity. It's hard to track all of them
to see if some of them are dirty. Also it's better to update them at the same time trying
to minimize number of requests to database and cache. To solve this problem simple use Flusher:

```go
package main

import "github.com/summer-solutions/orm"

func main() {

   //.. register pools and entities
 
    type TestEntity struct {
        Orm                  *orm.ORM
        ID                   uint
        Name                 string
    }
    registry := &orm.Registry{}
   //.. register pools and entities
    config, err := registry.CreateConfig()
    engine := orm.NewEngine(config)

    /* In this case flusher will keep maximum 10000 entities. If you add more it will panic */
    flusher := orm.Flusher{}

    var entity1 TestEntity
    var entity2 TestEntity
    entity3 := TestEntity{Name: "Hello"}
    err := engine.GetByID(1, &entity1)
    err = engine.GetByID(2, &entity2)
    flusher.RegisterEntity(&entity1, &entity2, &entity3)
   
    entity1.Name = "New Name"
    entity1.Orm.MarkToDelete()
    
    err = flusher.Flush(engine) //executes all queries at once
    //or
    entities, err := flusher.FlushAndReturn(engine)

    /* 
        in this case flusher will keep maximum 10000 entities. 
        If you add more it automatically flush all of them and unregister them in flusher 
    */
    autoFlusher := orm.AutoFlusher()
    
    var entities []*TestEntity
    pager := orm.Pager{CurrentPage: 1, PageSize: 100}
    where := orm.NewWhere("1")
    for {
        err = engine.Search(where, pager, &entities)
        for _, entity := range entities {
          entity.Name = "New Name"
          err = autoFlusher.RegisterEntity(&entity) //it will auto flush every 10 iterations
        }
        pager.IncrementPage()
        if len(entities) < pager.GetPageSize() {
            break
        }
    }
    err = autoFlusher.Flush(engine)
    if err != nil {
       ///...
    }
    /* to change limit, but try to avoid bug numbers */
    flusher.Limit = 20000
    /* to use flushLazy */
    flusher.Lazy = true
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
        ID                   uint64
        Name                 string
        School               *orm.ReferenceOne  `orm:"ref=SchoolEntity"`
    }
    
    type SchoolEntity struct {
        Orm                  *orm.ORM
        ID                   uint64
        Name                 string
    }

    type UserHouse struct {
        Orm                  *orm.ORM
        ID                   uint64
        User                 *orm.ReferenceOne  `orm:"ref=UserEntity;cascade"`
    }
    registry := &orm.Registry{}
   //.. register pools and entities
    config, err := registry.CreateConfig()
    engine := orm.NewEngine(config)


    /* 
        Orm will add index and foreign key automatically. By default 'ON DELETE RESTRICT' but
        you can add tag "cascade" to force 'ON DELETE CASCADE'
    */
    
    school := SchoolEntity{Name: "Name of school"}
    err := engine.Flush(&school)
    if err != nil {
       ///...
    }
    
    user := UserEntity{Name: "John"}
    user.School.ID = school.ID
    err = engine.Flush(&user)
    if err != nil {
       ///...
    }

    /* accessing reference */
    user.School.Has() //returns true
    has, err := user.School.Load(engine, &school) //has is true
    
    /* deleting reference */
    user.School.ID = 0
    err = engine.Flush(&user)
    if err != nil {
       ///...
    }

    /* if you don't have ID yet you can still assign references */
    school = SchoolEntity{Name: "New School"}
    user.School.Reference = &school
    err = engine.Flush(&user, &school)
    if err != nil {
       ///...
    }

}

```


## Cached queries

```go
package main

import "github.com/summer-solutions/orm"

func main() {

   //.. register pools and entities
 
    type UserEntity struct {
        Orm                  *orm.ORM
        ID                   uint64
        Name                 string
        Age                  uint16
        IndexAge             *orm.CachedQuery `query:":Age = ? ORDER BY :ID"`
        IndexAll             *orm.CachedQuery `query:""`
        IndexName            *orm.CachedQuery `queryOne:":Name = ?"`
    }
    registry := &orm.Registry{}
    //.. register pools and entities
    config, err := registry.CreateConfig()
    engine := orm.NewEngine(config)
    
    user := UserEntity{Name: "John", Age: 18}
    err := engine.Flush(&user)
    pager := orm.Pager{CurrentPage: 1, PageSize: 100}
    var users []*UserEntity
    totalRows, err := engine.CachedSearch(&users, "IndexAge", pager, 18)
    totalRows, err = engine.CachedSearch(&users, "IndexAll", pager)
    has, err := engine.CachedSearchOne(&user, "IndexName", "John")

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

    registry := &orm.Registry{}
    registry.RegisterMySQLPool("root:root@tcp(localhost:3306)/database_name")
    registry.RegisterRedis("localhost:6379", 3, "queues_pool")
    registry.RegisterLazyQueue("default", "queues_pool") //if not defined orm is using default redis pool

 
    type UserEntity struct {
        Orm                  *orm.ORM
        ID                   uint64
        Name                 string
    }
    config, err := registry.CreateConfig()
    engine := orm.NewEngine(config)
    
    user := UserEntity{Name: "John"}
    var user2 UserEntity
    err := engine.GetByID(1, &user2)
    user2.Orm.MarkToDelete()
    err = engine.FlushLazy(&user, &user2)
    
    /* you can use Flusher also */
    flusher := &LazyFlusher(100, true)
    user = UserEntity{Name: "Bob"}
    err = flusher.RegisterEntity(&user)
    err = flusher.Flush(engine)
    if err != nil {
       ///...
    }
    
    /* you should run a thread that is receiving lazy queries */
    lazyReceiver := orm.NewLazyReceiver{engine, "queues_pool"}
    for {
        has, err = lazyReceiver.Digest()
        if err != nil {
           ///...
        }
        if !has {
            //sleep x seconds
        }   
    }
}

```

## Flush in cache

Sometimes you are changing entity very often and you don't want to flush changes
to database every time entity is changed. If entity is using shared cached (redis) you
can use FlushInCache feature. When entity is changed new data is stored in cache but 
from time to time FlushFromCacheReceiver is flushing all differences between cache and database.

```go
package main

import "github.com/summer-solutions/orm"

func main() {

    registry := &orm.Registry{}

    registry.RegisterMySQLPool("root:root@tcp(localhost:3306)/database_name")
    registry.RegisterRedis("localhost:6379", 3, "queues_pool")
    registry.RegisterLazyQueue("default", "queues_pool") //if not defined orm is using default redis pool
    //.. register entities

 
    type UserEntity struct {
        Orm                  *orm.ORM
        ID                   uint64
        Name                 string
    }
    
    config, err := registry.CreateConfig()
    engine := orm.NewEngine(config)
    
    var user UserEntity
    err := engine.GetByID(1, &user)
    user.Name = "New name"
    err = engine.FlushInCache(&user) //updated only in redis
    user.Name = "New name 2"
    err = engine.FlushInCache(&user) //updated only in redis
    
    /* you should run a thread that is flushing changes in database */
    lazyReceiver := orm.NewFlushFromCacheReceiver{engine, "queues_pool"}
    for {
        //in our case it will only one query:
        // UPDATE `UserEntity` SET `Name` = "New name 2" WHERE `ID` = 1
        has, err := lazyReceiver.Digest()
        if !has {
            //sleep x seconds
        }   
    }
}

```

## Set defaults

If you need to define default values for entity simply extend orm.DefaultValuesInterface.

```go
func main() {

    type UserEntity struct {
        Orm                  *orm.ORM
        ID                   uint64
        Name                 string
    }

    func (e *UserEntity) SetDefaults() {
        e.Name = "Tom"
    }
    
}

```

## Fake delete

If you want to keep deleted entity in database but ny default this entity should be excluded
from all engine.Search() and engine.CacheSearch() queries you can use FakeDelete column. Simply create
field bool with name "FakeDelete".

```go
func main() {

    type UserEntity struct {
        Orm                  *orm.ORM
        ID                   uint64
        Name                 string
        FakeDelete           bool
    }

    //you can delete in two ways:
    user.Orm.MarkToDelete() -> will set user.FakeDelete = true
    or:
    user.FakeDelete = true

    engine.Flush(user) //it will save entity id in Column `FakeDelete`.

    //will return all rows where `FakeDelete` = 0
    total, err = engine.SearchWithCount(orm.NewWhere("1"), nil, &rows)

    //To force delete (remove row from DB):
    user.Orm.ForceMarkToDelete()
    engine.Flush(user)
}


```

## Validate

If you need to define validation for entity simply extend orm.ValidateInterface.

```go
func main() {

    type UserEntity struct {
        Orm                  *orm.ORM
        ID                   uint64
        Name                 string
    }

    func (e *UserEntity) Validate() error {
        if e.Name == "Tom" {
            return fmt.Errorf("Tom is not allowed")
        }
        return nil
    }
    
}

```

## After saved

If you need to execute code after entity is added or updated simply extend orm.AfterSavedInterface.

```go
func main() {

    type UserEntity struct {
        Orm                  *orm.ORM
        ID                   uint64
        Value                int
        Calculated           string `orm:"ignore"`
    }

    func (e *UserEntity) AfterSaved(engine *orm.Engine) error {
        e.Calculated = e.Value + 1
        return nil
    }
}

```

## Working with Redis

```go
package main

import "github.com/summer-solutions/orm"

func main() {

    config := &orm.Config{}
    config.RegisterRedis("localhost:6379", 0)
    engine := orm.NewEngine(config)
    
    //storing data in cached for x seconds
    val, err := engine.GetRedis().GetSet("key", 1, func() interface{} {
		return "hello"
	})
    
    //standard redis api
    keys, err := engine.GetRedis().LRange("key", 1, 2)
    err = engine.GetRedis().LPush("key", "a", "b")
    //...
}

```


## Working with local cache

```go
package main

import "github.com/summer-solutions/orm"

func main() {
    
    registry := &orm.Registry{}
    registry.RegisterLocalCache(1000)
    config, err := registry.CreateConfig()
    engine := orm.NewEngine(config)
    
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
    values := engine.GetLocalCache().HMget("key")
    
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

    registry := &orm.Registry{}
    registry.RegisterMySQLPool("root:root@tcp(localhost:3306)/database_name")
    config, err := registry.CreateConfig()
    engine := orm.NewEngine(config)

    res, err := engine.GetMysql().Exec("UPDATE `table_name` SET `Name` = ? WHERE `ID` = ?", "Hello", 2)

    var row string
    err = engine.GetMysql().QueryRow("SELECT * FROM `table_name` WHERE  `ID` = ?", 1).Scan(&row)
    if err != nil {
        if err != sql.ErrNoRows {
            ///...
        }
        //no row found
    }
    
    results, err := engine.GetMysql().Query("SELECT * FROM `table_name` WHERE  `ID` > ? LIMIT 100", 1)
    for results.Next() {
    	var row string
        err = results.Scan(&row)
    }

}

```

## Working with Locker

Shared cached that is using redis

```go
package main

import "github.com/summer-solutions/orm"

func main() {

    registry := &orm.Registry{}
    registry.RegisterRedis("localhost:6379", 0)
    registry.RegisterLocker("default", "default")
    config, err :=  registry.CreateConfig()
    engine := orm.NewEngine(config)
    
    locker, _ := engine.GetLocker()
    lock, err := locker.Obtain("my_lock", 1 * Time.Second)
    if err != nil {
        panic(err)
    }
    defer lock.Release()
    
    // do smth
    
    ttl, err := lock.TTL()
    if ttl == 0 {
        panic("lock lost")
    }
}

```