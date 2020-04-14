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
	return fmt.Sprintf("entity '%s' is not registered", strings.Trim(e.Name, "*[]"))
}

type Config struct {
	tableSchemas         map[reflect.Type]*TableSchema
	entities             map[string]reflect.Type
	sqlClients           map[string]*DBConfig
	dirtyQueues          map[string]DirtyQueueSender
	lazyQueuesCodes      map[string]string
	localCacheContainers map[string]*LocalCacheConfig
	redisServers         map[string]*RedisCacheConfig
	lockServers          map[string]string
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
