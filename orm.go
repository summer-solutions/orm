package orm

import (
	"container/list"
	"database/sql"
	"fmt"
	"github.com/go-redis/redis/v7"
	"github.com/golang/groupcache/lru"
	"reflect"
)

var sqlClients = make(map[string]*DB)
var localCacheContainers = make(map[string]*LocalCache)
var redisServers = make(map[string]*RedisCache)
var entities = make(map[string]reflect.Type)
var enums = make(map[string]reflect.Value)
var dirtyQueuesCodes = make(map[string]string)
var dirtyQueuesCodesNames = make([]string, 0)
var lazyQueuesCodes = make(map[string]string)
var lazyQueuesCodesNames = make([]string, 0)

func RegisterEntity(entity ...interface{}) {
	for _, e := range entity {
		t := reflect.TypeOf(e)
		entities[t.String()] = t
	}
}

func UnregisterEntities() {
	entities = make(map[string]reflect.Type)
}

func RegisterEnum(name string, enum interface{}) {
	enums[name] = reflect.Indirect(reflect.ValueOf(enum))
}

func Init(entity ...interface{}) {
	for _, e := range entity {
		value := reflect.ValueOf(e)
		initIfNeeded(value)
	}
}

func initIfNeeded(value reflect.Value) *ORM {
	elem := value.Elem()
	orm := elem.Field(0).Interface().(*ORM)
	if orm == nil {
		orm = &ORM{dBData: make(map[string]interface{}), elem: elem}
		elem.Field(0).Set(reflect.ValueOf(orm))
		tableSchema := getTableSchema(elem.Type())
		for _, code := range tableSchema.refOne {
			reference := tableSchema.Tags[code]["ref"]
			def := ReferenceOne{t: GetEntityType(reference)}
			elem.FieldByName(code).Set(reflect.ValueOf(&def))
		}
		defaultInterface, is := value.Interface().(DefaultValuesInterface)
		if is {
			defaultInterface.SetDefaults()
		}
	}
	return orm
}

func RegisterMySqlPool(dataSourceName string, code ...string) error {
	return registerSqlPool(dataSourceName, code...)
}

func UnregisterSqlPools() {
	sqlClients = make(map[string]*DB)
}

func RegisterLocalCache(size int, code ...string) {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	localCacheContainers[dbCode] = &LocalCache{code: dbCode, lru: lru.New(size)}
}

func RegisterRedis(address string, db int, code ...string) *RedisCache {
	client := redis.NewClient(&redis.Options{
		Addr: address,
		DB:   db,
	})
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	redisCache := &RedisCache{code: dbCode, client: client}
	redisServers[dbCode] = redisCache
	return redisCache
}

func RegisterDirtyQueue(code string, redisCode string) {
	dirtyQueuesCodes[code] = redisCode
	dirtyQueuesCodesNames = append(dirtyQueuesCodesNames, code)
}

func GetDirtyQueueCodes() []string {
	return dirtyQueuesCodesNames
}

func RegisterLazyQueue(code string, redisCode string) {
	lazyQueuesCodes[code] = redisCode
	lazyQueuesCodesNames = append(lazyQueuesCodesNames, code)
}

func GetLazyQueueCodes() []string {
	return lazyQueuesCodesNames
}

func GetMysql(code ...string) *DB {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	db, has := sqlClients[dbCode]
	if !has {
		panic(fmt.Errorf("unregistered database code: %s", dbCode))
	}
	return db
}

func GetLocalCache(code ...string) *LocalCache {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	cache, has := localCacheContainers[dbCode]
	if has == true {
		return cache
	}
	panic(fmt.Errorf("unregistered local cache: %s", dbCode))
}

func GetRedis(code ...string) *RedisCache {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	client, has := redisServers[dbCode]
	if !has {
		panic(fmt.Errorf("unregistered redis code: %s", dbCode))
	}
	return client
}

func getRedisForQueue(code string) *RedisCache {
	queueCode := code + "_queue"
	client, has := redisServers[queueCode]
	if !has {
		panic(fmt.Errorf("unregistered redis queue: %s", code))
	}
	return client
}

func GetEntityType(name string) reflect.Type {
	t, has := entities[name]
	if !has {
		panic(fmt.Errorf("unregistered entity %s", name))
	}
	return t
}

func RegisterDatabaseLogger(logger DatabaseLogger) []*list.Element {
	res := make([]*list.Element, 0)
	for _, db := range sqlClients {
		res = append(res, db.RegisterLogger(logger))
	}
	return res
}

func UnregisterDatabaseLoggers(elements ...*list.Element) {
	for _, db := range sqlClients {
		for _, e := range elements {
			db.UnregisterLogger(e)
		}
	}
}

func RegisterRedisLogger(logger CacheLogger) []*list.Element {
	res := make([]*list.Element, 0)
	for _, red := range redisServers {
		res = append(res, red.RegisterLogger(logger))
	}
	return res
}

func UnregisterRedisLoggers(elements ...*list.Element) {
	for _, red := range redisServers {
		for _, e := range elements {
			red.UnregisterLogger(e)
		}
	}
}

func registerSqlPool(dataSourceName string, code ...string) error {
	sqlDB, _ := sql.Open("mysql", dataSourceName)
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	db := &DB{code: dbCode, db: sqlDB}
	sqlClients[dbCode] = db

	var dbName string
	err := db.QueryRow("SELECT DATABASE()").Scan(&dbName)
	if err != nil {
		return err
	}
	db.databaseName = dbName
	return nil
}
