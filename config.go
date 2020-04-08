package orm

import (
	"fmt"
	"reflect"
	"strings"
)

type EntityNotRegisteredError struct {
	Name string
}

func (e EntityNotRegisteredError) Error() string {
	return fmt.Sprintf("entity is not registered %s", strings.Trim(e.Name, "*[]"))
}

type DBPoolNotRegisteredError struct {
	Name string
}

func (e DBPoolNotRegisteredError) Error() string {
	return fmt.Sprintf("db pool %s is not registered", e.Name)
}

type LocalCachePoolNotRegisteredError struct {
	Name string
}

func (e LocalCachePoolNotRegisteredError) Error() string {
	return fmt.Sprintf("local cache pool %s is not registered", e.Name)
}

type RedisCachePoolNotRegisteredError struct {
	Name string
}

func (e RedisCachePoolNotRegisteredError) Error() string {
	return fmt.Sprintf("redis cache pool %s is not registered", e.Name)
}

type Config struct {
	tableSchemas         map[reflect.Type]*TableSchema
	entities             map[string]reflect.Type
	sqlClients           map[string]*DBConfig
	dirtyQueues          map[string]DirtyQueueSender
	lazyQueuesCodes      map[string]string
	localCacheContainers map[string]*LocalCacheConfig
	redisServers         map[string]*RedisCacheConfig
	enums                map[string]reflect.Value
}

func (c *Config) GetTableSchema(entityOrTypeOrName interface{}) (tableSchema *TableSchema, has bool) {
	asString, is := entityOrTypeOrName.(string)
	var val interface{}
	if is {
		t, has := c.getEntityType(asString)
		if !has {
			return nil, false
		}
		val = t
	} else {
		val = entityOrTypeOrName
	}
	tableSchema = getTableSchema(c, val)
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
