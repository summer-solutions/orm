package orm

import (
	"fmt"
	"os"
	"reflect"

	"github.com/apex/log"
	"github.com/apex/log/handlers/multi"
	"github.com/apex/log/handlers/text"
)

type Engine struct {
	registry               *validatedRegistry
	dbs                    map[string]*DB
	localCache             map[string]*LocalCache
	redis                  map[string]*RedisCache
	locks                  map[string]*Locker
	logMetaData            map[string]interface{}
	trackedEntities        []reflect.Value
	trackedEntitiesCounter int
	log                    *log.Entry
	logHandler             *multi.Handler
}

func (e *Engine) AddLogger(handler log.Handler) {
	e.logHandler.Handlers = append(e.logHandler.Handlers, handler)
}

func (e *Engine) SetLogLevel(level log.Level) {
	logger := log.Logger{Handler: e.logHandler, Level: level}
	e.log = logger.WithField("source", "orm")
	for _, db := range e.dbs {
		db.log = e.log
	}
	for _, r := range e.redis {
		r.log = e.log
	}
}

func (e *Engine) EnableDebug() {
	e.AddLogger(text.New(os.Stdout))
	e.SetLogLevel(log.DebugLevel)
}

func (e *Engine) SetLogMetaData(key string, value interface{}) {
	if e.logMetaData == nil {
		e.logMetaData = make(map[string]interface{})
	}
	e.logMetaData[key] = value
}

func (e *Engine) Track(entity ...Entity) {
	for _, element := range entity {
		value := e.getValue(element)
		initIfNeeded(e, value)
		e.trackedEntities = append(e.trackedEntities, value)
		e.trackedEntitiesCounter++
		if e.trackedEntitiesCounter == 100000 {
			panic(fmt.Errorf("track limit 100000 excedded"))
		}
	}
}

func (e *Engine) TrackAndFlush(entity ...Entity) error {
	e.Track(entity...)
	return e.Flush()
}

func (e *Engine) Flush() error {
	return e.flushTrackedEntities(false)
}

func (e *Engine) FlushLazy() error {
	return e.flushTrackedEntities(true)
}

func (e *Engine) ClearTrackedEntities() {
	e.trackedEntities = make([]reflect.Value, 0)
}

func (e *Engine) MarkToDelete(entity ...Entity) {
	for _, row := range entity {
		e.Track(row)
		initEntityIfNeeded(e, row)
		if row.getTableSchema().hasFakeDelete {
			row.getElem().FieldByName("FakeDelete").SetBool(true)
			continue
		}
		row.getDBData()["_delete"] = true
	}
}

func (e *Engine) ForceMarkToDelete(entity ...Entity) {
	for _, row := range entity {
		initEntityIfNeeded(e, row)
		row.getDBData()["_delete"] = true
		e.Track(row)
	}
}

func (e *Engine) IsDirty(entity Entity) bool {
	if !entity.Loaded() {
		return true
	}
	initEntityIfNeeded(e, entity)
	is, _, _ := getDirtyBind(entity.getElem())
	return is
}

func (e *Engine) GetRegistry() ValidatedRegistry {
	return e.registry
}

func (e *Engine) GetMysql(code ...string) *DB {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	db, has := e.dbs[dbCode]
	if !has {
		panic(fmt.Errorf("unregistered mysql pool '%s'", dbCode))
	}
	return db
}

func (e *Engine) GetLocalCache(code ...string) *LocalCache {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	cache, has := e.localCache[dbCode]
	if !has {
		panic(fmt.Errorf("unregistered local cache pool '%s'", dbCode))
	}
	return cache
}

func (e *Engine) GetRedis(code ...string) *RedisCache {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	cache, has := e.redis[dbCode]
	if !has {
		panic(fmt.Errorf("unregistered redis cache pool '%s'", dbCode))
	}
	return cache
}

func (e *Engine) GetLocker(code ...string) *Locker {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	locker, has := e.locks[dbCode]
	if !has {
		panic(fmt.Errorf("unregistered locker pool '%s'", dbCode))
	}
	return locker
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

func (e *Engine) CachedSearchOne(entity Entity, indexName string, arguments ...interface{}) (has bool, err error) {
	return cachedSearchOne(e, entity, indexName, arguments, nil)
}

func (e *Engine) CachedSearchOneWithReferences(entity Entity, indexName string, arguments []interface{}, references []string) (has bool, err error) {
	return cachedSearchOne(e, entity, indexName, arguments, references)
}

func (e *Engine) CachedSearch(entities interface{}, indexName string, pager *Pager, arguments ...interface{}) (totalRows int, err error) {
	return cachedSearch(e, entities, indexName, pager, arguments, nil)
}

func (e *Engine) CachedSearchWithReferences(entities interface{}, indexName string, pager *Pager,
	arguments []interface{}, references []string) (totalRows int, err error) {
	return cachedSearch(e, entities, indexName, pager, arguments, references)
}

func (e *Engine) ClearByIDs(entity Entity, ids ...uint64) error {
	return clearByIDs(e, entity, ids...)
}

func (e *Engine) FlushInCache(entities ...interface{}) error {
	return flushInCache(e, entities...)
}

func (e *Engine) LoadByID(id uint64, entity Entity, references ...string) (found bool, err error) {
	return loadByID(e, id, entity, references...)
}

func (e *Engine) Load(entity Entity, references ...string) error {
	if entity.Loaded() {
		return nil
	}
	initEntityIfNeeded(e, entity)
	id := entity.getElem().Field(1).Uint()
	if id > 0 {
		_, err := loadByID(e, id, entity, references...)
		return err
	}
	return nil
}

func (e *Engine) LoadByIDs(ids []uint64, entities interface{}, references ...string) (missing []uint64, err error) {
	return tryByIDs(e, ids, reflect.ValueOf(entities).Elem(), references)
}

func (e *Engine) GetAlters() (alters []Alter, err error) {
	return getAlters(e)
}

func (e *Engine) getRedisForQueue(code string) *RedisCache {
	return e.GetRedis(code + "_queue")
}

func (e *Engine) getValue(entity Entity) reflect.Value {
	value := reflect.ValueOf(entity)
	if value.Kind() != reflect.Ptr {
		panic(fmt.Errorf("registered entity '%s' is not a poninter", value.Type().String()))
	}
	initIfNeeded(e, value)
	return value
}

func (e *Engine) flushTrackedEntities(lazy bool) error {
	err := flush(e, lazy, e.trackedEntities...)
	if err != nil {
		return err
	}
	e.trackedEntities = make([]reflect.Value, 0)
	e.trackedEntitiesCounter = 0
	return nil
}
