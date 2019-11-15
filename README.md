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
    orm.SetRedisForQueue("default_queue") //if not defined orm is using default redis pool


    /* Local cache (in memory) */

    orm.RegisterLocalCache(1000) //you need to define cache size
    //optionally you can define pool name as second argument
    orm.RegisterLocalCache(100, "second_pool")

    /* Context cache (explain later) */
    orm.EnableContextCache(100, 1)

}

```