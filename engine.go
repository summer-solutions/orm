package orm

import (
	"fmt"
	"reflect"
)

type Engine struct {
	config     *Config
	dbs        map[string]*DB
	localCache map[string]*LocalCache
	redis      map[string]*RedisCache
	locks      map[string]*Locker
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

func (e *Engine) LoadByIDs(ids []uint64, entities interface{}, references ...string) (missing []uint64, err error) {
	return tryByIDs(e, ids, reflect.ValueOf(entities).Elem(), references)
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
