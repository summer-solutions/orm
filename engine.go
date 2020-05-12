package orm

import (
	"fmt"
	"os"
	"reflect"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/apex/log"
	"github.com/apex/log/handlers/multi"
	"github.com/apex/log/handlers/text"
)

type Engine struct {
	registry                     *validatedRegistry
	dbs                          map[string]*DB
	localCache                   map[string]*LocalCache
	redis                        map[string]*RedisCache
	locks                        map[string]*Locker
	rabbitMQChannels             map[string]*rabbitMQChannel
	rabbitMQQueues               map[string]*RabbitMQQueue
	rabbitMQDelayedQueues        map[string]*RabbitMQDelayedQueue
	rabbitMQRouters              map[string]*RabbitMQRouter
	logMetaData                  map[string]interface{}
	trackedEntities              []Entity
	trackedEntitiesCounter       int
	log                          *log.Entry
	logHandler                   *multi.Handler
	afterCommitLocalCacheSets    map[string][]interface{}
	afterCommitRedisCacheDeletes map[string][]string
}

func (e *Engine) Defer() {
	for _, channel := range e.rabbitMQChannels {
		channel.close()
	}
}

func (e *Engine) AddLogger(handler log.Handler) {
	e.logHandler.Handlers = append(e.logHandler.Handlers, handler)
}

func (e *Engine) SetLogLevel(level log.Level) {
	logger := log.Logger{Handler: e.logHandler, Level: level}
	e.log = logger.WithField("source", "orm")
	e.log.Level = level
	for _, db := range e.dbs {
		db.log = e.log
	}
	for _, r := range e.redis {
		r.log = e.log
	}
	for _, l := range e.localCache {
		l.log = e.log
	}
	for _, l := range e.locks {
		l.log = e.log
	}
	for _, l := range e.rabbitMQChannels {
		l.log = e.log
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
	for _, entity := range entity {
		initIfNeeded(e, entity)
		e.trackedEntities = append(e.trackedEntities, entity)
		e.trackedEntitiesCounter++
		if e.trackedEntitiesCounter == 10000 {
			panic(fmt.Errorf("track limit 10000 exceeded"))
		}
	}
}

func (e *Engine) TrackAndFlush(entity ...Entity) error {
	e.Track(entity...)
	return e.Flush()
}

func (e *Engine) Flush() error {
	return e.flushTrackedEntities(false, false)
}

func (e *Engine) FlushLazy() error {
	return e.flushTrackedEntities(true, false)
}

func (e *Engine) FlushInTransaction() error {
	return e.flushTrackedEntities(false, true)
}

func (e *Engine) FlushWithLock(lockerPool string, lockName string, ttl time.Duration, waitTimeout time.Duration) error {
	return e.flushWithLock(false, lockerPool, lockName, ttl, waitTimeout)
}

func (e *Engine) FlushInTransactionWithLock(lockerPool string, lockName string, ttl time.Duration, waitTimeout time.Duration) error {
	return e.flushWithLock(true, lockerPool, lockName, ttl, waitTimeout)
}

func (e *Engine) ClearTrackedEntities() {
	e.trackedEntities = make([]Entity, 0)
}

func (e *Engine) SetOnDuplicateKeyUpdate(update *Where, entity ...Entity) {
	for _, row := range entity {
		orm := initIfNeeded(e, row)
		orm.attributes.onDuplicateKeyUpdate = update
	}
}

func (e *Engine) SetEntityLogMeta(key string, value interface{}, entity ...Entity) {
	for _, row := range entity {
		orm := initIfNeeded(e, row)
		if orm.attributes.logMeta == nil {
			orm.attributes.logMeta = make(map[string]interface{})
		}
		orm.attributes.logMeta[key] = value
	}
}

func (e *Engine) MarkToDelete(entity ...Entity) {
	for _, row := range entity {
		e.Track(row)
		orm := initIfNeeded(e, row)
		if orm.tableSchema.hasFakeDelete {
			orm.attributes.elem.FieldByName("FakeDelete").SetBool(true)
			continue
		}
		orm.attributes.delete = true
	}
}

func (e *Engine) ForceMarkToDelete(entity ...Entity) {
	for _, row := range entity {
		orm := initIfNeeded(e, row)
		orm.attributes.delete = true
		e.Track(row)
	}
}

func (e *Engine) MarkDirty(entity Entity, queueCode string, ids ...uint64) error {
	_, has := e.GetRegistry().GetDirtyQueues()[queueCode]
	if !has {
		return fmt.Errorf("unknown dirty queue '%s'", queueCode)
	}
	channel := e.GetRabbitMQQueue("dirty_queue_" + queueCode)
	entityName := initIfNeeded(e, entity).tableSchema.t.String()
	for _, id := range ids {
		val := &DirtyQueueValue{Updated: true, ID: id, EntityName: entityName}
		asJSON, _ := jsoniter.ConfigFastest.Marshal(val)
		err := channel.Publish(asJSON)
		if err != nil {
			return err
		}
	}
	return nil
}

func (e *Engine) Loaded(entity Entity) bool {
	orm := initIfNeeded(e, entity)
	return orm.attributes.loaded
}

func (e *Engine) IsDirty(entity Entity) bool {
	if !e.Loaded(entity) {
		return true
	}
	initIfNeeded(e, entity)
	is, _ := getDirtyBind(entity)
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

func (e *Engine) GetRabbitMQQueue(code string) *RabbitMQQueue {
	queue, has := e.rabbitMQQueues[code]
	if has {
		return queue
	}
	channel, has := e.rabbitMQChannels[code]
	if !has {
		panic(fmt.Errorf("unregistered rabbitMQ queue '%s'", code))
	}
	if channel.config.Router != "" {
		panic(fmt.Errorf("rabbitMQ queue '%s' is declared as router", code))
	}
	if e.rabbitMQQueues == nil {
		e.rabbitMQQueues = make(map[string]*RabbitMQQueue)
	}
	e.rabbitMQQueues[code] = &RabbitMQQueue{channel}
	return e.rabbitMQQueues[code]
}

func (e *Engine) GetRabbitMQDelayedQueue(code string) *RabbitMQDelayedQueue {
	queue, has := e.rabbitMQDelayedQueues[code]
	if has {
		return queue
	}
	channel, has := e.rabbitMQChannels[code]
	if !has {
		panic(fmt.Errorf("unregistered rabbitMQ delayed queue '%s'", code))
	}
	if channel.config.Router == "" {
		panic(fmt.Errorf("rabbitMQ queue '%s' is not declared as delayed queue", code))
	}
	if !channel.config.Delayed {
		panic(fmt.Errorf("rabbitMQ queue '%s' is not declared as delayed queue", code))
	}
	if e.rabbitMQDelayedQueues == nil {
		e.rabbitMQDelayedQueues = make(map[string]*RabbitMQDelayedQueue)
	}
	e.rabbitMQDelayedQueues[code] = &RabbitMQDelayedQueue{channel}
	return e.rabbitMQDelayedQueues[code]
}

func (e *Engine) GetRabbitMQRouter(code string) *RabbitMQRouter {
	queue, has := e.rabbitMQRouters[code]
	if has {
		return queue
	}
	channel, has := e.rabbitMQChannels[code]
	if !has {
		panic(fmt.Errorf("unregistered rabbitMQ router '%s'", code))
	}
	if channel.config.Router == "" {
		panic(fmt.Errorf("rabbitMQ queue '%s' is not declared as router", code))
	}
	if channel.config.Delayed {
		panic(fmt.Errorf("rabbitMQ queue '%s' is declared as delayed queue", code))
	}
	if e.rabbitMQRouters == nil {
		e.rabbitMQRouters = make(map[string]*RabbitMQRouter)
	}
	e.rabbitMQRouters[code] = &RabbitMQRouter{channel}
	return e.rabbitMQRouters[code]
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

func (e *Engine) SearchIDs(where *Where, pager *Pager, entity Entity) ([]uint64, error) {
	results, _, err := searchIDs(true, e, where, pager, false, reflect.TypeOf(entity).Elem())
	return results, err
}

func (e *Engine) SearchOne(where *Where, entity Entity, references ...string) (bool, error) {
	return searchOne(true, e, where, entity, references)
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

func (e *Engine) FlushInCache(entities ...Entity) error {
	return flushInCache(e, entities...)
}

func (e *Engine) LoadByID(id uint64, entity Entity, references ...string) (found bool, err error) {
	return loadByID(e, id, entity, true, references...)
}

func (e *Engine) Load(entity Entity, references ...string) error {
	if e.Loaded(entity) {
		if len(references) > 0 {
			orm := entity.getORM()
			return warmUpReferences(e, orm.tableSchema, orm.attributes.elem, references, false)
		}
		return nil
	}
	orm := initIfNeeded(e, entity)
	id := orm.GetID()
	if id > 0 {
		_, err := loadByID(e, id, entity, true, references...)
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

func (e *Engine) flushTrackedEntities(lazy bool, transaction bool) error {
	if e.trackedEntitiesCounter == 0 {
		return nil
	}
	var dbPools map[string]*DB
	if transaction {
		dbPools = make(map[string]*DB)
		for _, entity := range e.trackedEntities {
			db := entity.getORM().tableSchema.GetMysql(e)
			dbPools[db.code] = db
		}
		for _, db := range dbPools {
			err := db.Begin()
			if err != nil {
				return err
			}
		}
	}
	defer func() {
		for _, db := range dbPools {
			db.Rollback()
		}
	}()

	err := flush(e, lazy, transaction, e.trackedEntities...)
	if err != nil {
		return err
	}
	if transaction {
		for _, db := range dbPools {
			err := db.Commit()
			if err != nil {
				return err
			}
		}
	}
	e.trackedEntities = make([]Entity, 0)
	e.trackedEntitiesCounter = 0
	return nil
}

func (e *Engine) flushWithLock(transaction bool, lockerPool string, lockName string, ttl time.Duration, waitTimeout time.Duration) error {
	locker := e.GetLocker(lockerPool)
	lock, has, err := locker.Obtain(lockName, ttl, waitTimeout)
	if err != nil {
		return err
	}
	if !has {
		return fmt.Errorf("lock wait timeout")
	}
	defer lock.Release()
	return e.flushTrackedEntities(false, transaction)
}
