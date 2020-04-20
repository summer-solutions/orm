package orm

import (
	"fmt"
	"reflect"

	"github.com/bsm/redislock"
)

type Engine struct {
	config     *Config
	dbs        map[string]*DB
	localCache map[string]*LocalCache
	redis      map[string]*RedisCache
	locks      map[string]*Locker
}

func NewEngine(config *Config) *Engine {
	e := &Engine{config: config}
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

func (e *Engine) RegisterNewEntity(entityReference interface{}) {
	value := reflect.ValueOf(entityReference)
	if value.Kind() != reflect.Ptr {
		panic(fmt.Errorf("registered entity '%s' is not a poninter", value.Type().String()))
	}
	initIfNeeded(e, value, true)
}

func (e *Engine) GetConfig() *Config {
	return e.config
}

func (e *Engine) GetMysql(code ...string) (db *DB, has bool) {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	db, has = e.dbs[dbCode]
	return db, has
}

func (e *Engine) GetLocalCache(code ...string) (cache *LocalCache, has bool) {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	cache, has = e.localCache[dbCode]
	return cache, has
}

func (e *Engine) GetRedis(code ...string) (cache *RedisCache, has bool) {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	cache, has = e.redis[dbCode]
	return cache, has
}

func (e *Engine) GetLocker(code ...string) (locker *Locker, has bool) {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	locker, has = e.locks[dbCode]
	return locker, has
}

func (e *Engine) SearchWithCount(where *Where, pager *Pager, entities interface{}, references ...string) (totalRows int, err error) {
	return search(true, e, where, pager, true, reflect.ValueOf(entities).Elem(), references...)
}

func (e *Engine) Search(where *Where, pager *Pager, entities interface{}, references ...string) error {
	_, err := search(true, e, where, pager, false, reflect.ValueOf(entities).Elem(), references...)
	return err
}

func (e *Engine) SearchIDsWithCount(where *Where, pager *Pager, entity interface{}) (results []uint64, totalRows int, err error) {
	return searchIDsWithCount(true, e, where, pager, reflect.TypeOf(entity))
}

func (e *Engine) SearchIDs(where *Where, pager *Pager, entity interface{}) ([]uint64, error) {
	results, _, err := searchIDs(true, e, where, pager, false, reflect.TypeOf(entity))
	return results, err
}

func (e *Engine) SearchOne(where *Where, entity interface{}) (bool, error) {
	return searchOne(true, e, where, entity)
}

func (e *Engine) GetByIDs(ids []uint64, entities interface{}, references ...string) error {
	return getByIDs(e, ids, entities, references...)
}

func (e *Engine) TryByIDs(ids []uint64, entities interface{}, references ...string) (missing []uint64, err error) {
	return tryByIDs(e, ids, reflect.ValueOf(entities).Elem(), references)
}

func (e *Engine) CachedSearchOne(entity interface{}, indexName string, arguments ...interface{}) (has bool, err error) {
	return cachedSearchOne(e, entity, indexName, arguments...)
}

func (e *Engine) CachedSearch(entities interface{}, indexName string, pager *Pager, arguments ...interface{}) (totalRows int, err error) {
	return cachedSearch(e, entities, indexName, pager, arguments, nil)
}

func (e *Engine) CachedSearchWithReferences(entities interface{}, indexName string, pager *Pager,
	arguments []interface{}, references []string) (totalRows int, err error) {
	return cachedSearch(e, entities, indexName, pager, arguments, references)
}

func (e *Engine) ClearByIDs(entity interface{}, ids ...uint64) error {
	return clearByIDs(e, entity, ids...)
}

func (e *Engine) FlushInCache(entities ...interface{}) error {
	return flushInCache(e, entities...)
}

func (e *Engine) LoadByID(id uint64, entity interface{}, references ...string) (found bool, err error) {
	return loadByID(e, id, entity, references...)
}

func (e *Engine) GetAlters() (alters []Alter, err error) {
	return getAlters(e)
}

func (e *Engine) RegisterDatabaseLogger(logger DatabaseLogger) {
	for _, db := range e.dbs {
		db.RegisterLogger(logger)
	}
}

func (e *Engine) RegisterRedisLogger(logger CacheLogger) {
	for _, red := range e.redis {
		red.RegisterLogger(logger)
	}
}

func (e *Engine) getRedisForQueue(code string) (*RedisCache, bool) {
	return e.GetRedis(code + "_queue")
}
