package orm

import (
	"database/sql"
	"math"
	"reflect"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/golang/groupcache/lru"
	"github.com/juju/errors"
)

type Registry struct {
	sqlClients           map[string]*DBConfig
	localCacheContainers map[string]*LocalCacheConfig
	redisServers         map[string]*RedisCacheConfig
	entities             map[string]reflect.Type
	enums                map[string]reflect.Value
	dirtyQueues          map[string]DirtyQueueSender
	lazyQueuesCodes      map[string]string
	locks                map[string]string
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
		db, err := sql.Open("mysql", v.dataSourceName)
		if err != nil {
			return nil, err
		}
		var maxConnections int
		var skip string
		err = db.QueryRow("SHOW VARIABLES LIKE 'max_connections'").Scan(&skip, &maxConnections)
		if err != nil {
			return nil, errors.Annotatef(err, "can't connect to mysql '%s'", v.code)
		}
		var waitTimeout int
		err = db.QueryRow("SHOW VARIABLES LIKE 'wait_timeout'").Scan(&skip, &waitTimeout)
		if err != nil {
			return nil, err
		}
		maxConnections = int(math.Floor(float64(maxConnections) * 0.9))
		if maxConnections == 0 {
			maxConnections = 1
		}
		maxIdleConnections := int(math.Floor(float64(maxConnections) * 0.2))
		if maxIdleConnections == 0 {
			maxIdleConnections = 2
		}
		waitTimeout = int(math.Floor(float64(waitTimeout) * 0.8))
		if waitTimeout == 0 {
			waitTimeout = 1
		}
		db.SetMaxOpenConns(maxConnections)
		db.SetMaxIdleConns(maxIdleConnections)
		db.SetConnMaxLifetime(time.Duration(waitTimeout) * time.Second)
		v.db = db
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

	if config.lockServers == nil {
		config.lockServers = make(map[string]string)
	}
	for k, v := range r.locks {
		config.lockServers[k] = v
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
		tableSchema, err := initTableSchema(r, entityType)
		if err != nil {
			return nil, err
		}
		config.tableSchemas[entityType] = tableSchema
		config.entities[name] = entityType
	}
	engine := NewEngine(config)
	for _, schema := range config.tableSchemas {
		_, err := checkStruct(schema, engine, schema.t, make(map[string]*index), make(map[string]*foreignIndex), "")
		if err != nil {
			return nil, errors.Annotatef(err, "invalid entity struct '%s'", schema.t.String())
		}
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

func (r *Registry) RegisterMySQLPool(dataSourceName string, code ...string) {
	r.registerSQLPool(dataSourceName, code...)
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

func (r *Registry) RegisterLocker(code string, redisCode string) {
	if r.locks == nil {
		r.locks = make(map[string]string)
	}
	r.locks[code] = redisCode
}

func (r *Registry) registerSQLPool(dataSourceName string, code ...string) {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	db := &DBConfig{code: dbCode, dataSourceName: dataSourceName}
	if r.sqlClients == nil {
		r.sqlClients = make(map[string]*DBConfig)
	}
	parts := strings.Split(dataSourceName, "/")
	db.databaseName = parts[len(parts)-1]
	r.sqlClients[dbCode] = db
}
