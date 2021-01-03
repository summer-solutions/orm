package orm

import (
	"encoding/json"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/golang/groupcache/lru"

	logApex "github.com/apex/log"

	levelHandler "github.com/apex/log/handlers/level"
	"github.com/juju/errors"

	"github.com/apex/log/handlers/text"
)

type obj struct {
	ID         uint64
	StorageKey string
	Data       interface{}
}

type Engine struct {
	registry                     *validatedRegistry
	dbs                          map[string]*DB
	clickHouseDbs                map[string]*ClickHouse
	localCache                   map[string]*LocalCache
	redis                        map[string]*RedisCache
	elastic                      map[string]*Elastic
	locks                        map[string]*Locker
	rabbitMQChannels             map[string]*rabbitMQChannel
	rabbitMQQueues               map[string]*RabbitMQQueue
	rabbitMQRouters              map[string]*RabbitMQRouter
	logMetaData                  map[string]interface{}
	dataLoader                   *dataLoader
	hasRequestCache              bool
	trackedEntities              []Entity
	trackedEntitiesCounter       int
	queryLoggers                 map[QueryLoggerSource]*logger
	log                          *log
	afterCommitLocalCacheSets    map[string][]interface{}
	afterCommitRedisCacheDeletes map[string][]string
	afterCommitDataLoaderSets    dataLoaderSets
	afterCommitDirtyQueues       map[string][]*DirtyQueueValue
	afterCommitLogQueues         []*LogQueueValue
	dataDog                      *dataDog
}

func (e *Engine) DataDog() DataDog {
	return e.dataDog
}

func (e *Engine) Log() Log {
	if e.log == nil {
		e.log = newLog(e)
	}
	return e.log
}

func (e *Engine) EnableRequestCache(goroutines bool) {
	if goroutines {
		e.dataLoader = &dataLoader{engine: e, maxBatchSize: dataLoaderMaxPatch}
	} else {
		e.hasRequestCache = true
	}
}

func (e *Engine) EnableLogger(level logApex.Level, handlers ...logApex.Handler) {
	if len(handlers) == 0 {
		handlers = []logApex.Handler{&jsonHandler{}}
	}
	for _, handler := range handlers {
		e.Log().(*log).logger.handler.Handlers = append(e.log.logger.handler.Handlers, levelHandler.New(handler, level))
	}
}

func (e *Engine) EnableDebug() {
	e.Log().(*log).logger.handler.Handlers = append(e.log.logger.handler.Handlers, levelHandler.New(text.New(os.Stderr), logApex.DebugLevel))
}

func (e *Engine) AddQueryLogger(handler logApex.Handler, level logApex.Level, source ...QueryLoggerSource) {
	if e.queryLoggers == nil {
		e.queryLoggers = make(map[QueryLoggerSource]*logger)
	}
	if len(source) == 0 {
		source = []QueryLoggerSource{QueryLoggerSourceDB, QueryLoggerSourceRedis, QueryLoggerSourceRabbitMQ, QueryLoggerSourceElastic, QueryLoggerSourceClickHouse}
	}
	newHandler := levelHandler.New(handler, level)
	for _, source := range source {
		l, has := e.queryLoggers[source]
		if has {
			l.handler.Handlers = append(l.handler.Handlers, newHandler)
		} else {
			e.queryLoggers[source] = e.newLogger(newHandler, level)
		}
	}
}

func (e *Engine) EnableQueryDebug(source ...QueryLoggerSource) {
	e.AddQueryLogger(text.New(os.Stdout), logApex.DebugLevel, source...)
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
	e.flushTrackedEntities(false, false, true)
}

func (e *Engine) FlushWithCheck() error {
	return e.flushWithCheck(false)
}

func (e *Engine) FlushInTransactionWithCheck() error {
	return e.flushWithCheck(true)
}

func (e *Engine) FlushWithFullCheck() error {
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				e.ClearTrackedEntities()
				asErr := r.(error)
				err = asErr
			}
		}()
		e.flushTrackedEntities(false, false, false)
	}()
	return err
}

func (e *Engine) FlushLazy() {
	e.flushTrackedEntities(true, false, false)
}

func (e *Engine) FlushInTransaction() {
	e.flushTrackedEntities(false, true, false)
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

func (e *Engine) SetOnDuplicateKeyUpdate(update *Where, entity Entity) {
	initIfNeeded(e, entity).attributes.onDuplicateKeyUpdate = update
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
	entityName := initIfNeeded(e, entity).tableSchema.t.String()
	for _, id := range ids {
		val := &DirtyQueueValue{Updated: true, ID: id, EntityName: entityName}
		asJSON, _ := json.Marshal(val)
		valChannel := &redis.XAddArgs{Stream: dirtyChannelPrefix + queueCode, ID: "*", Values: []string{"v", string(asJSON)}}
		e.GetRedis().XAdd(valChannel)
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
	is, _ := e.GetDirtyBind(entity)
	return is
}

func (e *Engine) GetDirtyBind(entity Entity) (bool, map[string]interface{}) {
	initIfNeeded(e, entity)
	return getDirtyBind(entity)
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
		if dbCode == requestCacheKey {
			cache = &LocalCache{code: dbCode, engine: e, m: &sync.Mutex{}, lru: lru.New(5000)}
			e.localCache[dbCode] = cache
			return cache
		}
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

func (e *Engine) GetElastic(code ...string) *Elastic {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	elastic, has := e.elastic[dbCode]
	if !has {
		panic(errors.Errorf("unregistered elastic pool '%s'", dbCode))
	}
	return elastic
}

func (e *Engine) GetClickHouse(code ...string) *ClickHouse {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	ch, has := e.clickHouseDbs[dbCode]
	if !has {
		panic(errors.Errorf("unregistered clickhouse pool '%s'", dbCode))
	}
	return ch
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

func (e *Engine) GetRabbitMQRouter(queueName string) *RabbitMQRouter {
	queue, has := e.rabbitMQRouters[queueName]
	if has {
		return queue
	}
	channel, has := e.rabbitMQChannels[queueName]
	if !has {
		panic(errors.Errorf("unregistered rabbitMQ router '%s'. Use queue name, not router name.", queueName))
	}
	if channel.config.Router == "" {
		panic(errors.Errorf("rabbitMQ queue '%s' is not declared as router", queueName))
	}
	if e.rabbitMQRouters == nil {
		e.rabbitMQRouters = make(map[string]*RabbitMQRouter)
	}
	e.rabbitMQRouters[queueName] = &RabbitMQRouter{channel}
	return e.rabbitMQRouters[queueName]
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

func (e *Engine) SearchIDsWithCount(where *Where, pager *Pager, entity Entity) (results []uint64, totalRows int) {
	return searchIDsWithCount(true, e, where, pager, reflect.TypeOf(entity).Elem())
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
	total, _ := cachedSearch(e, entities, indexName, pager, arguments, nil)
	return total
}

func (e *Engine) CachedSearchIDs(entity Entity, indexName string, pager *Pager, arguments ...interface{}) (totalRows int, ids []uint64) {
	return cachedSearch(e, entity, indexName, pager, arguments, nil)
}

func (e *Engine) CachedSearchWithReferences(entities interface{}, indexName string, pager *Pager,
	arguments []interface{}, references []string) (totalRows int) {
	total, _ := cachedSearch(e, entities, indexName, pager, arguments, references)
	return total
}

func (e *Engine) ClearByIDs(entity Entity, ids ...uint64) {
	clearByIDs(e, entity, ids...)
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

func (e *Engine) GetElasticIndexAlters() (alters []ElasticIndexAlter) {
	return getElasticIndexAlters(e)
}
