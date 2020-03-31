package orm

import (
	"database/sql"
	"fmt"
	"github.com/go-redis/redis/v7"
	"github.com/golang/groupcache/lru"
	"reflect"
	"strings"
)

type Config struct {
	tableSchemas          map[reflect.Type]*TableSchema
	sqlClients            map[string]*DBConfig
	localCacheContainers  map[string]*LocalCacheConfig
	redisServers          map[string]*RedisCacheConfig
	entities              map[string]reflect.Type
	enums                 map[string]reflect.Value
	dirtyQueuesCodes      map[string]string
	dirtyQueuesCodesNames []string
	lazyQueuesCodes       map[string]string
	lazyQueuesCodesNames  []string
}

func (c *Config) RegisterEntity(entity ...interface{}) {
	if c.entities == nil {
		c.entities = make(map[string]reflect.Type)
	}
	for _, e := range entity {
		t := reflect.TypeOf(e)
		c.entities[t.String()] = t
	}
}

func (c *Config) RegisterEnum(name string, enum interface{}) {
	if c.enums == nil {
		c.enums = make(map[string]reflect.Value)
	}
	c.enums[name] = reflect.Indirect(reflect.ValueOf(enum))
}

func (c *Config) RegisterMySqlPool(dataSourceName string, code ...string) error {
	return c.registerSqlPool(dataSourceName, code...)
}

func (c *Config) RegisterLocalCache(size int, code ...string) {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	if c.localCacheContainers == nil {
		c.localCacheContainers = make(map[string]*LocalCacheConfig)
	}
	c.localCacheContainers[dbCode] = &LocalCacheConfig{code: dbCode, lru: lru.New(size)}
}

func (c *Config) RegisterRedis(address string, db int, code ...string) {
	client := redis.NewClient(&redis.Options{
		Addr: address,
		DB:   db,
	})
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	redisCache := &RedisCacheConfig{code: dbCode, client: client}
	if c.redisServers == nil {
		c.redisServers = make(map[string]*RedisCacheConfig)
	}
	c.redisServers[dbCode] = redisCache
}

func (c *Config) RegisterDirtyQueue(code string, redisCode string) {
	if c.dirtyQueuesCodes == nil {
		c.dirtyQueuesCodes = make(map[string]string)
	}
	c.dirtyQueuesCodes[code] = redisCode
	if c.dirtyQueuesCodesNames == nil {
		c.dirtyQueuesCodesNames = make([]string, 0)
	}
	c.dirtyQueuesCodesNames = append(c.dirtyQueuesCodesNames, code)
}

func (c *Config) GetTableSchema(entityOrType interface{}) *TableSchema {
	return getTableSchema(c, entityOrType)
}

func (c *Config) GetDirtyQueueCodes() []string {
	if c.dirtyQueuesCodes == nil {
		c.dirtyQueuesCodesNames = make([]string, 0)
	}
	return c.dirtyQueuesCodesNames
}

func (c *Config) RegisterLazyQueue(code string, redisCode string) {
	if c.lazyQueuesCodes == nil {
		c.lazyQueuesCodes = make(map[string]string)
	}
	c.lazyQueuesCodes[code] = redisCode
	if c.lazyQueuesCodesNames == nil {
		c.lazyQueuesCodesNames = make([]string, 0)
	}
	c.lazyQueuesCodesNames = append(c.lazyQueuesCodesNames, code)
}

func (c *Config) GetLazyQueueCodes() []string {
	if c.lazyQueuesCodesNames == nil {
		c.lazyQueuesCodesNames = make([]string, 0)
	}
	return c.lazyQueuesCodesNames
}

func (c *Config) GetEntityType(name string) reflect.Type {
	t, has := c.entities[name]
	if !has {
		return nil
	}
	return t
}

func (c *Config) Validate() error {
	for _, entity := range c.entities {
		_, err := getTableSchemaFromValue(c, entity)
		if err != nil {
			return err
		}
	}
	return nil
}

func (c *Config) getRedisForQueue(code string) (*RedisCacheConfig, error) {
	queueCode := code + "_queue"
	client, has := c.redisServers[queueCode]
	if !has {
		return nil, fmt.Errorf("unregistered redis queue: %s", code)
	}
	return client, nil
}

func (c *Config) registerSqlPool(dataSourceName string, code ...string) error {
	sqlDB, _ := sql.Open("mysql", dataSourceName)
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	db := &DBConfig{code: dbCode, db: sqlDB}
	if c.sqlClients == nil {
		c.sqlClients = make(map[string]*DBConfig)
	}
	c.sqlClients[dbCode] = db
	db.databaseName = strings.Split(dataSourceName, "/")[1]
	return nil
}
