package orm

import (
	"fmt"
	"reflect"
	"strings"

	"github.com/bsm/redislock"
)

type EntityNotRegisteredError struct {
	Name string
}

func (e EntityNotRegisteredError) Error() string {
	return fmt.Sprintf("entity '%s' is not registered", strings.Trim(e.Name, "*[]"))
}

type ValidatedRegistry interface {
	CreateEngine() *Engine
	GetTableSchema(entityName string) TableSchema
	GetTableSchemaForEntity(entity Entity) TableSchema
	GetDirtyQueues() map[string]int
}

type validatedRegistry struct {
	tableSchemas            map[reflect.Type]*tableSchema
	entities                map[string]reflect.Type
	sqlClients              map[string]*DBConfig
	clickHouseClients       map[string]*ClickHouseConfig
	dirtyQueues             map[string]int
	localCacheContainers    map[string]*LocalCacheConfig
	redisServers            map[string]*RedisCacheConfig
	elasticServers          map[string]*ElasticConfig
	rabbitMQServers         map[string]*rabbitMQConnection
	rabbitMQChannelsToQueue map[string]*rabbitMQChannelToQueue
	rabbitMQRouterConfigs   map[string]*RabbitMQRouterConfig
	lockServers             map[string]string
	enums                   map[string]Enum
}

func (r *validatedRegistry) CreateEngine() *Engine {
	e := &Engine{registry: r}
	e.dataDog = &dataDog{engine: e}
	e.dbs = make(map[string]*DB)
	e.trackedEntities = make([]Entity, 0)
	if e.registry.sqlClients != nil {
		for key, val := range e.registry.sqlClients {
			e.dbs[key] = &DB{engine: e, code: val.code, databaseName: val.databaseName,
				client: &standardSQLClient{db: val.db}}
		}
	}
	if e.registry.clickHouseClients != nil {
		for key, val := range e.registry.clickHouseClients {
			e.clickHouseDbs[key] = &ClickHouse{engine: e, code: val.code, client: val.db}
		}
	}
	e.localCache = make(map[string]*LocalCache)
	if e.registry.localCacheContainers != nil {
		for key, val := range e.registry.localCacheContainers {
			e.localCache[key] = &LocalCache{engine: e, code: val.code, lru: val.lru, ttl: val.ttl}
		}
	}
	e.redis = make(map[string]*RedisCache)
	if e.registry.redisServers != nil {
		for key, val := range e.registry.redisServers {
			e.redis[key] = &RedisCache{engine: e, code: val.code, client: &standardRedisClient{val.client}}
		}
	}
	e.elastic = make(map[string]*Elastic)
	if e.registry.elasticServers != nil {
		for key, val := range e.registry.elasticServers {
			e.elastic[key] = &Elastic{engine: e, code: val.code, client: val.client}
		}
	}

	e.rabbitMQChannels = make(map[string]*rabbitMQChannel)
	if e.registry.rabbitMQChannelsToQueue != nil {
		for key, val := range e.registry.rabbitMQChannelsToQueue {
			e.rabbitMQChannels[key] = &rabbitMQChannel{engine: e, connection: val.connection, config: val.config}
		}
	}

	e.locks = make(map[string]*Locker)
	if e.registry.lockServers != nil {
		for key, val := range e.registry.lockServers {
			locker := &standardLockerClient{client: redislock.New(e.registry.redisServers[val].client)}
			e.locks[key] = &Locker{locker: locker, code: val, engine: e}
		}
	}
	return e
}

func (r *validatedRegistry) GetTableSchema(entityName string) TableSchema {
	t, has := r.entities[entityName]
	if !has {
		return nil
	}
	return getTableSchema(r, t)
}

func (r *validatedRegistry) GetTableSchemaForEntity(entity Entity) TableSchema {
	t := reflect.TypeOf(entity)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	tableSchema := getTableSchema(r, t)
	if tableSchema == nil {
		panic(EntityNotRegisteredError{Name: t.String()})
	}
	return tableSchema
}

func (r *validatedRegistry) GetEnum(code string) Enum {
	return r.enums[code]
}

func (r *validatedRegistry) GetDirtyQueues() map[string]int {
	return r.dirtyQueues
}
