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

type Config struct {
	tableSchemas         map[reflect.Type]*TableSchema
	entities             map[string]reflect.Type
	sqlClients           map[string]*DBConfig
	dirtyQueues          map[string]DirtyQueueSender
	logQueues            map[string]LogQueueSender
	lazyQueuesCodes      map[string]string
	localCacheContainers map[string]*LocalCacheConfig
	redisServers         map[string]*RedisCacheConfig
	lockServers          map[string]string
	enums                map[string]reflect.Value
}

func (c *Config) CreateEngine() *Engine {
	e := &Engine{config: c}
	e.dbs = make(map[string]*DB)
	if e.config.sqlClients != nil {
		for key, val := range e.config.sqlClients {
			e.dbs[key] = &DB{engine: e, code: val.code, databaseName: val.databaseName, db: &sqlDBStandard{db: val.db}}
		}
	}
	e.localCache = make(map[string]*LocalCache)
	if e.config.localCacheContainers != nil {
		for key, val := range e.config.localCacheContainers {
			e.localCache[key] = &LocalCache{engine: e, code: val.code, lru: val.lru, ttl: val.ttl}
		}
	}
	e.redis = make(map[string]*RedisCache)
	if e.config.redisServers != nil {
		for key, val := range e.config.redisServers {
			e.redis[key] = &RedisCache{engine: e, code: val.code, client: val.client}
		}
	}
	e.locks = make(map[string]*Locker)
	if e.config.lockServers != nil {
		for key, val := range e.config.lockServers {
			locker := redislock.New(e.config.redisServers[val].client)
			e.locks[key] = &Locker{locker: locker}
		}
	}
	return e
}

func (c *Config) GetTableSchema(entityName string) (tableSchema *TableSchema, has bool) {
	t, has := c.getEntityType(entityName)
	if !has {
		return nil, false
	}
	tableSchema = getTableSchema(c, t)
	return tableSchema, tableSchema != nil
}

func (c *Config) GetDirtyQueueCodes() []string {
	codes := make([]string, len(c.dirtyQueues))
	i := 0
	for code := range c.dirtyQueues {
		codes[i] = code
		i++
	}
	return codes
}

func (c *Config) GetLogQueueCodes() []string {
	codes := make([]string, len(c.logQueues))
	i := 0
	for code := range c.logQueues {
		codes[i] = code
		i++
	}
	return codes
}

func (c *Config) GetLazyQueueCodes() []string {
	codes := make([]string, len(c.lazyQueuesCodes))
	i := 0
	for code := range c.lazyQueuesCodes {
		codes[i] = code
		i++
	}
	return codes
}

func (c *Config) getEntityType(name string) (t reflect.Type, has bool) {
	t, is := c.entities[name]
	if !is {
		return nil, false
	}
	return t, true
}
