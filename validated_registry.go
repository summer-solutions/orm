package orm

import (
	"context"
	"fmt"
	"reflect"
	"sort"
)

type ValidatedRegistry interface {
	CreateEngine() *Engine
	GetTableSchema(entityName string) TableSchema
	GetTableSchemaForEntity(entity Entity) TableSchema
	GetSourceRegistry() *Registry
	GetEnum(code string) Enum
	GetEnums() map[string]Enum
	GetRedisStreams() map[string]map[string][]string
	GetRedisPools() []string
	GetEntities() map[string]reflect.Type
}

type validatedRegistry struct {
	registry             *Registry
	tableSchemas         map[reflect.Type]*tableSchema
	entities             map[string]reflect.Type
	redisSearchIndexes   map[string]*RedisSearchIndex
	sqlClients           map[string]*DBConfig
	clickHouseClients    map[string]*ClickHouseConfig
	localCacheContainers map[string]*LocalCacheConfig
	redisServers         map[string]*RedisCacheConfig
	redisStreamGroups    map[string]map[string]map[string]bool
	redisStreamPools     map[string]string
	elasticServers       map[string]*ElasticConfig
	lockServers          map[string]string
	enums                map[string]Enum
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

func (r *validatedRegistry) GetRedisStreams() map[string]map[string][]string {
	res := make(map[string]map[string][]string)
	for redisPool, row := range r.redisStreamGroups {
		res[redisPool] = make(map[string][]string)
		for stream, groups := range row {
			res[redisPool][stream] = make([]string, len(groups))
			i := 0
			for group := range groups {
				res[redisPool][stream][i] = group
				i++
			}
		}
	}
	return res
}

func (r *validatedRegistry) GetRedisPools() []string {
	pools := make([]string, 0)
	groupedByAddress := make(map[string][]string)
	for code, v := range r.redisServers {
		groupedByAddress[v.address] = append(groupedByAddress[v.address], code)
	}
	for _, codes := range groupedByAddress {
		sort.Strings(codes)
		pools = append(pools, codes[0])
	}
	return pools
}

func (r *validatedRegistry) CreateEngine() *Engine {
	return &Engine{registry: r, context: context.Background()}
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
