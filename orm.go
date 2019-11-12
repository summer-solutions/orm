package orm

import (
	"database/sql"
	"fmt"
	"github.com/go-redis/redis/v7"
	"github.com/golang/groupcache/lru"
	"reflect"
	"time"
)

var mysqlPoolCodes = make([]string, 0)
var mySqlClients = make(map[string]*DB)
var localCacheContainers = make(map[string]*LocalCache)
var redisServers = make(map[string]*RedisCache)
var entities = make(map[string]reflect.Type)
var dirtyQueuesCodes = make(map[string]string)
var queueRedisName = "default"

func RegisterEntity(entity ...interface{}) {
	for _, e := range entity {
		t := reflect.TypeOf(e)
		entities[t.String()] = t
	}
}

func Init(entity ...interface{}) {
	for _, e := range entity {
		value := reflect.Indirect(reflect.ValueOf(e))
		initIfNeeded(value, e)
	}
}

func initIfNeeded(value reflect.Value, entity interface{}) *ORM {
	orm := value.Field(0).Interface().(*ORM)
	if orm == nil {
		orm = &ORM{dBData: make(map[string]interface{}), e: entity}
		value.Field(0).Set(reflect.ValueOf(orm))
	}
	orm.e = entity
	return orm
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

func EnableContextCache(size int, ttl int64) {
	localCacheContainers["_context_cache"] = &LocalCache{code: "_context_cache", lru: lru.New(size), ttl: ttl, created: time.Now().Unix()}
}

func DisableContextCache() {
	delete(localCacheContainers, "_context_cache")
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

func RegisterDirtyQueue(code string, redisCode string) {
	dirtyQueuesCodes[code] = redisCode
}

func SetRedisForQueue(redisCode string) {
	queueRedisName = redisCode
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

func getContextCache() *LocalCache {
	contextCache, has := localCacheContainers["_context_cache"]
	if !has {
		return nil
	}
	if time.Now().Unix()-contextCache.created+contextCache.ttl <= 0 {
		contextCache.lru.Clear()
		contextCache.created = time.Now().Unix()
	}
	return contextCache
}
