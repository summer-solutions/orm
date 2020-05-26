package orm

import (
	"encoding/json"
	"os"
	"reflect"
	"time"

	levelHandler "github.com/apex/log/handlers/level"
	"github.com/juju/errors"

	"github.com/apex/log"
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
	loggers                      map[LoggerSource]*logger
	afterCommitLocalCacheSets    map[string][]interface{}
	afterCommitRedisCacheDeletes map[string][]string
	dataDog                      *dataDog
}

func (e *Engine) DataDog() DataDog {
	return e.dataDog
}

func (e *Engine) AddLogger(handler log.Handler, level log.Level, source ...LoggerSource) {
	if e.loggers == nil {
		e.loggers = make(map[LoggerSource]*logger)
	}
	if len(source) == 0 {
		source = []LoggerSource{LoggerSourceDB, LoggerSourceRedis, LoggerSourceRabbitMQ}
	}
	newHandler := levelHandler.New(handler, level)
	for _, source := range source {
		l, has := e.loggers[source]
		if has {
			l.handler.Handlers = append(l.handler.Handlers, newHandler)
		} else {
			e.loggers[source] = e.newLogger(newHandler, level)
		}
	}
}

func (e *Engine) EnableDebug(source ...LoggerSource) {
	e.AddLogger(text.New(os.Stdout), log.DebugLevel, source...)
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
			panic(errors.Errorf("track limit 10000 exceeded"))
		}
	}
}

func (e *Engine) TrackAndFlush(entity ...Entity) {
	e.Track(entity...)
	e.Flush()
}

func (e *Engine) Flush() {
	e.flushTrackedEntities(false, false)
}

func (e *Engine) FlushWithCheck() (*ForeignKeyError, *DuplicatedKeyError) {
	var err1 *ForeignKeyError
	var err2 *DuplicatedKeyError
	func() {
		defer func() {
			if r := recover(); r != nil {
				asErr, is := r.(error)
				if !is {
					panic(r)
				}
				source := errors.Cause(asErr)
				assErr1, is := source.(*ForeignKeyError)
				if is {
					err1 = assErr1
					return
				}
				assErr2, is := source.(*DuplicatedKeyError)
				if is {
					err2 = assErr2
					return
				}
				panic(r)
			}
		}()
		e.flushTrackedEntities(false, false)
	}()
	return err1, err2
}

func (e *Engine) FlushLazy() {
	e.flushTrackedEntities(true, false)
}

func (e *Engine) FlushInTransaction() {
	e.flushTrackedEntities(false, true)
}

func (e *Engine) FlushWithLock(lockerPool string, lockName string, ttl time.Duration, waitTimeout time.Duration) {
	e.flushWithLock(false, lockerPool, lockName, ttl, waitTimeout)
}

func (e *Engine) FlushInTransactionWithLock(lockerPool string, lockName string, ttl time.Duration, waitTimeout time.Duration) {
	e.flushWithLock(true, lockerPool, lockName, ttl, waitTimeout)
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

func (e *Engine) MarkDirty(entity Entity, queueCode string, ids ...uint64) {
	_, has := e.GetRegistry().GetDirtyQueues()[queueCode]
	if !has {
		panic(errors.NotValidf("unknown dirty queue '%s'", queueCode))
	}
	channel := e.GetRabbitMQQueue("dirty_queue_" + queueCode)
	entityName := initIfNeeded(e, entity).tableSchema.t.String()
	for _, id := range ids {
		val := &DirtyQueueValue{Updated: true, ID: id, EntityName: entityName}
		asJSON, _ := json.Marshal(val)
		channel.Publish(asJSON)
	}
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
		panic(errors.Errorf("unregistered mysql pool '%s'", dbCode))
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
		panic(errors.Errorf("unregistered local cache pool '%s'", dbCode))
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
		panic(errors.Errorf("unregistered redis cache pool '%s'", dbCode))
	}
	return cache
}

func (e *Engine) GetRabbitMQQueue(queueName string) *RabbitMQQueue {
	queue, has := e.rabbitMQQueues[queueName]
	if has {
		return queue
	}
	channel, has := e.rabbitMQChannels[queueName]
	if !has {
		panic(errors.Errorf("unregistered rabbitMQ queue '%s'", queueName))
	}
	if channel.config.Router != "" {
		panic(errors.Errorf("rabbitMQ queue '%s' is declared as router", queueName))
	}
	if e.rabbitMQQueues == nil {
		e.rabbitMQQueues = make(map[string]*RabbitMQQueue)
	}
	e.rabbitMQQueues[queueName] = &RabbitMQQueue{channel}
	return e.rabbitMQQueues[queueName]
}

func (e *Engine) GetRabbitMQDelayedQueue(queueName string) *RabbitMQDelayedQueue {
	queue, has := e.rabbitMQDelayedQueues[queueName]
	if has {
		return queue
	}
	channel, has := e.rabbitMQChannels[queueName]
	if !has {
		panic(errors.Errorf("unregistered rabbitMQ delayed queue '%s'", queueName))
	}
	if channel.config.Router == "" {
		panic(errors.Errorf("rabbitMQ queue '%s' is not declared as delayed queue", queueName))
	}
	if !channel.config.Delayed {
		panic(errors.Errorf("rabbitMQ queue '%s' is not declared as delayed queue", queueName))
	}
	if e.rabbitMQDelayedQueues == nil {
		e.rabbitMQDelayedQueues = make(map[string]*RabbitMQDelayedQueue)
	}
	e.rabbitMQDelayedQueues[queueName] = &RabbitMQDelayedQueue{channel}
	return e.rabbitMQDelayedQueues[queueName]
}

func (e *Engine) GetRabbitMQRouter(channelName string) *RabbitMQRouter {
	queue, has := e.rabbitMQRouters[channelName]
	if has {
		return queue
	}
	channel, has := e.rabbitMQChannels[channelName]
	if !has {
		panic(errors.Errorf("unregistered rabbitMQ router '%s'", channelName))
	}
	if channel.config.Router == "" {
		panic(errors.Errorf("rabbitMQ queue '%s' is not declared as router", channelName))
	}
	if channel.config.Delayed {
		panic(errors.Errorf("rabbitMQ queue '%s' is declared as delayed queue", channelName))
	}
	if e.rabbitMQRouters == nil {
		e.rabbitMQRouters = make(map[string]*RabbitMQRouter)
	}
	e.rabbitMQRouters[channelName] = &RabbitMQRouter{channel}
	return e.rabbitMQRouters[channelName]
}

func (e *Engine) GetLocker(code ...string) *Locker {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	locker, has := e.locks[dbCode]
	if !has {
		panic(errors.Errorf("unregistered locker pool '%s'", dbCode))
	}
	return locker
}

func (e *Engine) SearchWithCount(where *Where, pager *Pager, entities interface{}, references ...string) (totalRows int) {
	return search(true, e, where, pager, true, reflect.ValueOf(entities).Elem(), references...)
}

func (e *Engine) Search(where *Where, pager *Pager, entities interface{}, references ...string) {
	search(true, e, where, pager, false, reflect.ValueOf(entities).Elem(), references...)
}

func (e *Engine) SearchIDsWithCount(where *Where, pager *Pager, entity interface{}) (results []uint64, totalRows int) {
	return searchIDsWithCount(true, e, where, pager, reflect.TypeOf(entity))
}

func (e *Engine) SearchIDs(where *Where, pager *Pager, entity Entity) []uint64 {
	results, _ := searchIDs(true, e, where, pager, false, reflect.TypeOf(entity).Elem())
	return results
}

func (e *Engine) SearchOne(where *Where, entity Entity, references ...string) (found bool) {
	return searchOne(true, e, where, entity, references)
}

func (e *Engine) CachedSearchOne(entity Entity, indexName string, arguments ...interface{}) (found bool) {
	return cachedSearchOne(e, entity, indexName, arguments, nil)
}

func (e *Engine) CachedSearchOneWithReferences(entity Entity, indexName string, arguments []interface{}, references []string) (found bool) {
	return cachedSearchOne(e, entity, indexName, arguments, references)
}

func (e *Engine) CachedSearch(entities interface{}, indexName string, pager *Pager, arguments ...interface{}) (totalRows int) {
	return cachedSearch(e, entities, indexName, pager, arguments, nil)
}

func (e *Engine) CachedSearchWithReferences(entities interface{}, indexName string, pager *Pager,
	arguments []interface{}, references []string) (totalRows int) {
	return cachedSearch(e, entities, indexName, pager, arguments, references)
}

func (e *Engine) ClearByIDs(entity Entity, ids ...uint64) {
	clearByIDs(e, entity, ids...)
}

func (e *Engine) FlushInCache(entities ...Entity) {
	flushInCache(e, entities...)
}

func (e *Engine) LoadByID(id uint64, entity Entity, references ...string) (found bool) {
	return loadByID(e, id, entity, true, references...)
}

func (e *Engine) Load(entity Entity, references ...string) {
	if e.Loaded(entity) {
		if len(references) > 0 {
			orm := entity.getORM()
			warmUpReferences(e, orm.tableSchema, orm.attributes.elem, references, false)
		}
		return
	}
	orm := initIfNeeded(e, entity)
	id := orm.GetID()
	if id > 0 {
		loadByID(e, id, entity, true, references...)
	}
}

func (e *Engine) LoadByIDs(ids []uint64, entities interface{}, references ...string) (missing []uint64) {
	return tryByIDs(e, ids, reflect.ValueOf(entities).Elem(), references)
}

func (e *Engine) GetAlters() (alters []Alter) {
	return getAlters(e)
}

func (e *Engine) flushTrackedEntities(lazy bool, transaction bool) {
	if e.trackedEntitiesCounter == 0 {
		return
	}
	var dbPools map[string]*DB
	if transaction {
		dbPools = make(map[string]*DB)
		for _, entity := range e.trackedEntities {
			db := entity.getORM().tableSchema.GetMysql(e)
			dbPools[db.code] = db
		}
		for _, db := range dbPools {
			db.Begin()
		}
	}
	defer func() {
		for _, db := range dbPools {
			db.Rollback()
		}
	}()

	flush(e, lazy, transaction, e.trackedEntities...)
	if transaction {
		for _, db := range dbPools {
			db.Commit()
		}
	}
	e.trackedEntities = make([]Entity, 0)
	e.trackedEntitiesCounter = 0
}

func (e *Engine) flushWithLock(transaction bool, lockerPool string, lockName string, ttl time.Duration, waitTimeout time.Duration) {
	locker := e.GetLocker(lockerPool)
	lock, has := locker.Obtain(lockName, ttl, waitTimeout)
	if !has {
		panic(errors.Timeoutf("lock wait timeout"))
	}
	defer lock.Release()
	e.flushTrackedEntities(false, transaction)
}
