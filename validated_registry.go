package orm

import (
	"context"
	"fmt"
	"reflect"
)

type ValidatedRegistry interface {
	CreateEngine() *Engine
	GetTableSchema(entityName string) TableSchema
	GetTableSchemaForEntity(entity Entity) TableSchema
	GetSourceRegistry() *Registry
	GetEnum(code string) Enum
	GetEnums() map[string]Enum
	GetRedisChannels() map[string]map[string]uint64
	GetEntities() map[string]reflect.Type
}

type validatedRegistry struct {
	registry                *Registry
	tableSchemas            map[reflect.Type]*tableSchema
	entities                map[string]reflect.Type
	sqlClients              map[string]*DBConfig
	clickHouseClients       map[string]*ClickHouseConfig
	localCacheContainers    map[string]*LocalCacheConfig
	redisServers            map[string]*RedisCacheConfig
	redisChannels           map[string]map[string]uint64
	elasticServers          map[string]*ElasticConfig
	rabbitMQServers         map[string]*rabbitMQConnection
	rabbitMQChannelsToQueue map[string]*rabbitMQChannelToQueue
	rabbitMQRouterConfigs   map[string]*RabbitMQRouterConfig
	lockServers             map[string]string
	enums                   map[string]Enum
}

func (r *validatedRegistry) GetSourceRegistry() *Registry {
	return r.registry
}

func (r *validatedRegistry) GetEntities() map[string]reflect.Type {
	return r.entities
}

func (r *validatedRegistry) GetEnums() map[string]Enum {
	return r.enums
}

func (r *validatedRegistry) GetRedisChannels() map[string]map[string]uint64 {
	return r.redisChannels
}

func (r *validatedRegistry) CreateEngine() *Engine {
	e := &Engine{registry: r, context: context.Background()}
	e.redis = make(map[string]*RedisCache)
	if e.registry.redisServers != nil {
		for key, val := range e.registry.redisServers {
			client := val.client
			if client != nil {
				client = client.WithContext(e.context)
			}
			ring := val.ring
			if ring != nil {
				ring = ring.WithContext(e.context)
			}
			e.redis[key] = &RedisCache{engine: e, code: val.code, client: &standardRedisClient{client, ring}}
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
		panic(fmt.Errorf("entity '%s' is not registered", t.String()))
	}
	return tableSchema
}

func (r *validatedRegistry) GetEnum(code string) Enum {
	return r.enums[code]
}
