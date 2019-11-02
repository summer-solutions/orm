package orm

import (
	"database/sql"
	"fmt"
	"github.com/go-redis/redis/v7"
	"github.com/golang/groupcache/lru"
	"reflect"
)

var mysqlPoolCodes = make([]string, 0)
var mySqlClients = make(map[string]*DB)
var localCacheContainers = make(map[string]*LocalCache)
var redisServers = make(map[string]*RedisCache)
var entities = make(map[string]reflect.Type)

func RegisterEntity(entity ...interface{}) {
	for _, e := range entity {
		t := reflect.TypeOf(e)
		entities[t.String()] = t
	}
}

func RegisterMySqlPool(code string, dataSourceName string) *DB {
	sqlDB, _ := sql.Open("mysql", dataSourceName)
	db := &DB{code: code, db: sqlDB}
	mySqlClients[code] = db
	mysqlPoolCodes = append(mysqlPoolCodes, code)
	return db
}

func UnregisterMySqlPools() {
	mySqlClients = make(map[string]*DB)
	mysqlPoolCodes = make([]string, 0)
}

func RegisterLocalCache(code string, size int) {
	localCacheContainers[code] = &LocalCache{code: code, lru: lru.New(size)}
}

func RegisterRedis(code string, address string, db int) *RedisCache {
	client := redis.NewClient(&redis.Options{
		Addr: address,
		DB:   db,
	})
	redisCache := &RedisCache{code: code, client: client}
	redisServers[code] = redisCache
	return redisCache
}

func GetMysqlDB(code string) *DB {

	db, has := mySqlClients[code]
	if !has {
		panic(fmt.Errorf("unregistered database code: %s", code))
	}
	return db
}

func GetLocalCacheContainer(code string) *LocalCache {
	cache, has := localCacheContainers[code]
	if has == true {
		return cache
	}
	if code == "default" {
		RegisterLocalCache("default", 10000)
		return GetLocalCacheContainer("default")
	}
	panic(fmt.Errorf("unregistered local cache: %s", code))
}

func GetRedisCache(code string) *RedisCache {

	client, has := redisServers[code]
	if !has {
		panic(fmt.Errorf("unregistered redis code: %s", code))
	}
	return client
}

func getEntityType(name string) reflect.Type {
	t, has := entities[name]
	if !has {
		panic(fmt.Errorf("unregistered entity %s", name))
	}
	return t
}
