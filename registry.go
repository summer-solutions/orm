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
	logQueues            map[string]LogQueueSender
	lazyQueuesCodes      map[string]string
	locks                map[string]string
}

func (r *Registry) Validate() (ValidatedRegistry, error) {
	registry := &validatedRegistry{}
	l := len(r.entities)
	registry.tableSchemas = make(map[reflect.Type]*tableSchema, l)
	registry.entities = make(map[string]reflect.Type)
	if registry.sqlClients == nil {
		registry.sqlClients = make(map[string]*DBConfig)
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
		registry.sqlClients[k] = v
	}
	if registry.dirtyQueues == nil {
		registry.dirtyQueues = make(map[string]DirtyQueueSender)
	}
	for k, v := range r.dirtyQueues {
		registry.dirtyQueues[k] = v
	}
	if registry.logQueues == nil {
		registry.logQueues = make(map[string]LogQueueSender)
	}
	for k, v := range r.logQueues {
		registry.logQueues[k] = v
	}
	if registry.lazyQueuesCodes == nil {
		registry.lazyQueuesCodes = make(map[string]string)
	}
	for k, v := range r.lazyQueuesCodes {
		registry.lazyQueuesCodes[k] = v
	}

	if registry.lockServers == nil {
		registry.lockServers = make(map[string]string)
	}
	for k, v := range r.locks {
		registry.lockServers[k] = v
	}

	if registry.localCacheContainers == nil {
		registry.localCacheContainers = make(map[string]*LocalCacheConfig)
	}
	for k, v := range r.localCacheContainers {
		registry.localCacheContainers[k] = v
	}
	if registry.redisServers == nil {
		registry.redisServers = make(map[string]*RedisCacheConfig)
	}
	for k, v := range r.redisServers {
		registry.redisServers[k] = v
	}
	if registry.enums == nil {
		registry.enums = make(map[string]reflect.Value)
	}
	for k, v := range r.enums {
		registry.enums[k] = v
	}
	for name, entityType := range r.entities {
		tableSchema, err := initTableSchema(r, entityType)
		if err != nil {
			return nil, err
		}
		registry.tableSchemas[entityType] = tableSchema
		registry.entities[name] = entityType
	}
	engine := registry.CreateEngine()
	for _, schema := range registry.tableSchemas {
		_, err := checkStruct(schema, engine, schema.t, make(map[string]*index), make(map[string]*foreignIndex), "")
		if err != nil {
			return nil, errors.Annotatef(err, "invalid entity struct '%s'", schema.t.String())
		}
	}
	return registry, nil
}

func (r *Registry) RegisterEntity(entity ...interface{}) {
	if r.entities == nil {
		r.entities = make(map[string]reflect.Type)
	}
	for _, e := range entity {
		t := reflect.TypeOf(e)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
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

func (r *Registry) RegisterLogQueue(code string, sender LogQueueSender) {
	if r.logQueues == nil {
		r.logQueues = make(map[string]LogQueueSender)
	}
	r.logQueues[code] = sender
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
