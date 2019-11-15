# orm

## Defining database and cache pool connections

```go
package main

import "github.com/summer-solutions/orm"

func main() {

    defer orm.Defer()
    orm.RegisterMySqlPool("root:root@tcp(localhost:3306)/database_name")
    //optionally you can define pool code name as second argument
    orm.RegisterMySqlPool("root:root@tcp(localhost:3307)/database_name", "second_pool")

}

```