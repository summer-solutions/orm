package orm

import (
	"database/sql"
	"github.com/go-redis/redis/v7"
	"github.com/golang/groupcache/lru"
	"reflect"
	"strings"
)

type Registry struct {
	sqlClients           map[string]*DBConfig
	localCacheContainers map[string]*LocalCacheConfig
	redisServers         map[string]*RedisCacheConfig
	entities             map[string]reflect.Type
	enums                map[string]reflect.Value
	dirtyQueues          map[string]DirtyQueueSender
	lazyQueuesCodes      map[string]string
}

func (r *Registry) CreateConfig() (*Config, error) {
	config := &Config{}
	l := len(r.entities)
	config.tableSchemas = make(map[reflect.Type]*TableSchema, l)
	config.entities = make(map[string]reflect.Type)
	if config.sqlClients == nil {
		config.sqlClients = make(map[string]*DBConfig)
	}
	for k, v := range r.sqlClients {
		config.sqlClients[k] = v
	}
	if config.dirtyQueues == nil {
		config.dirtyQueues = make(map[string]DirtyQueueSender)
	}
	for k, v := range r.dirtyQueues {
		config.dirtyQueues[k] = v
	}
	if config.lazyQueuesCodes == nil {
		config.lazyQueuesCodes = make(map[string]string)
	}
	for k, v := range r.lazyQueuesCodes {
		config.lazyQueuesCodes[k] = v
	}
	if config.localCacheContainers == nil {
		config.localCacheContainers = make(map[string]*LocalCacheConfig)
	}
	for k, v := range r.localCacheContainers {
		config.localCacheContainers[k] = v
	}
	if config.redisServers == nil {
		config.redisServers = make(map[string]*RedisCacheConfig)
	}
	for k, v := range r.redisServers {
		config.redisServers[k] = v
	}
	if config.enums == nil {
		config.enums = make(map[string]reflect.Value)
	}
	for k, v := range r.enums {
		config.enums[k] = v
	}
	for name, entityType := range r.entities {
		tableSchema, err := initTableSchema(entityType)
		if err != nil {
			return nil, err
		}
		config.tableSchemas[entityType] = tableSchema
		config.entities[name] = entityType
	}
	return config, nil
}

func (r *Registry) RegisterEntity(entity ...interface{}) {
	if r.entities == nil {
		r.entities = make(map[string]reflect.Type)
	}
	for _, e := range entity {
		t := reflect.TypeOf(e)
		r.entities[t.String()] = t
	}
}

func (r *Registry) RegisterEnum(name string, enum interface{}) {
	if r.enums == nil {
		r.enums = make(map[string]reflect.Value)
	}
	r.enums[name] = reflect.Indirect(reflect.ValueOf(enum))
}

func (r *Registry) RegisterMySqlPool(dataSourceName string, code ...string) {
	r.registerSqlPool(dataSourceName, code...)
}

func (r *Registry) RegisterLocalCache(size int, code ...string) {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	if r.localCacheContainers == nil {
		r.localCacheContainers = make(map[string]*LocalCacheConfig)
	}
	r.localCacheContainers[dbCode] = &LocalCacheConfig{code: dbCode, lru: lru.New(size)}
}

func (r *Registry) RegisterRedis(address string, db int, code ...string) {
	client := redis.NewClient(&redis.Options{
		Addr: address,
		DB:   db,
	})
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	redisCache := &RedisCacheConfig{code: dbCode, client: client}
	if r.redisServers == nil {
		r.redisServers = make(map[string]*RedisCacheConfig)
	}
	r.redisServers[dbCode] = redisCache
}

func (r *Registry) RegisterDirtyQueue(code string, sender DirtyQueueSender) {
	if r.dirtyQueues == nil {
		r.dirtyQueues = make(map[string]DirtyQueueSender)
	}
	r.dirtyQueues[code] = sender
}

func (r *Registry) RegisterLazyQueue(code string, redisCode string) {
	if r.lazyQueuesCodes == nil {
		r.lazyQueuesCodes = make(map[string]string)
	}
	r.lazyQueuesCodes[code] = redisCode
}

func (r *Registry) registerSqlPool(dataSourceName string, code ...string) {
	sqlDB, _ := sql.Open("mysql", dataSourceName)
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	db := &DBConfig{code: dbCode, db: sqlDB}
	if r.sqlClients == nil {
		r.sqlClients = make(map[string]*DBConfig)
	}
	parts := strings.Split(dataSourceName, "/")
	db.databaseName = parts[len(parts)-1]
	r.sqlClients[dbCode] = db
}
