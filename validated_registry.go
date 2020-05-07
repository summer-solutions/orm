package orm

import (
	"fmt"
	"os"
	"reflect"
	"strings"

	"github.com/apex/log"
	"github.com/apex/log/handlers/multi"
	"github.com/apex/log/handlers/text"

	"github.com/bsm/redislock"
)

type EntityNotRegisteredError struct {
	Name string
}

func (e EntityNotRegisteredError) Error() string {
	return fmt.Sprintf("entity '%s' is not registered", strings.Trim(e.Name, "*[]"))
}

type ValidatedRegistry interface {
	CreateEngine() *Engine
	GetTableSchema(entityName string) TableSchema
	GetTableSchemaForEntity(entity Entity) TableSchema
	GetDirtyQueueCodes() []string
	GetEntitiesForDirtyQueue(queueCode string) []string
	GetLazQueueSender() QueueSender
	GetLogQueueSenders() map[string]QueueSender
	AddLogger(handler log.Handler)
	SetLogLevel(level log.Level)
	EnableDebug()
}

type validatedRegistry struct {
	tableSchemas            map[reflect.Type]*tableSchema
	entities                map[string]reflect.Type
	sqlClients              map[string]*DBConfig
	dirtyQueues             map[string]QueueSender
	logQueues               map[string]QueueSender
	lazyQueue               QueueSender
	localCacheContainers    map[string]*LocalCacheConfig
	redisServers            map[string]*RedisCacheConfig
	rabbitMQServers         map[string]*rabbitMQConnection
	rabbitMQChannelsToQueue map[string]*rabbitMQChannelToQueue
	rabbitMQExchangeConfigs map[string]*RabbitMQExchangeConfig
	lockServers             map[string]string
	enums                   map[string]reflect.Value
	log                     *log.Entry
	logHandler              *multi.Handler
}

func (r *validatedRegistry) CreateEngine() *Engine {
	e := &Engine{registry: r}
	e.dbs = make(map[string]*DB)
	e.trackedEntities = make([]Entity, 0)
	e.log = r.log
	e.logHandler = multi.New()
	if r.logHandler != nil {
		e.logHandler.Handlers = r.logHandler.Handlers
	}
	if e.registry.sqlClients != nil {
		for key, val := range e.registry.sqlClients {
			logHandler := multi.New()
			if r.logHandler != nil {
				logHandler.Handlers = r.logHandler.Handlers
			}
			e.dbs[key] = &DB{engine: e, code: val.code, databaseName: val.databaseName,
				client: &standardSQLClient{db: val.db}, log: r.log, logHandler: logHandler}
		}
	}
	e.localCache = make(map[string]*LocalCache)
	if e.registry.localCacheContainers != nil {
		for key, val := range e.registry.localCacheContainers {
			logHandler := multi.New()
			if r.logHandler != nil {
				logHandler.Handlers = r.logHandler.Handlers
			}
			e.localCache[key] = &LocalCache{engine: e, code: val.code, lru: val.lru, ttl: val.ttl, log: r.log, logHandler: logHandler}
		}
	}
	e.redis = make(map[string]*RedisCache)
	if e.registry.redisServers != nil {
		for key, val := range e.registry.redisServers {
			logHandler := multi.New()
			if r.logHandler != nil {
				logHandler.Handlers = r.logHandler.Handlers
			}
			e.redis[key] = &RedisCache{engine: e, code: val.code, client: &standardRedisClient{val.client}, log: r.log, logHandler: logHandler}
		}
	}

	e.rabbitMQChannels = make(map[string]*RabbitMQChannel)
	if e.registry.rabbitMQChannelsToQueue != nil {
		for key, val := range e.registry.rabbitMQChannelsToQueue {
			logHandler := multi.New()
			if r.logHandler != nil {
				logHandler.Handlers = r.logHandler.Handlers
			}
			e.rabbitMQChannels[key] = &RabbitMQChannel{engine: e, connection: val.connection, config: val.config, log: r.log, logHandler: logHandler}
		}
	}

	e.locks = make(map[string]*Locker)
	if e.registry.lockServers != nil {
		for key, val := range e.registry.lockServers {
			logHandler := multi.New()
			if r.logHandler != nil {
				logHandler.Handlers = r.logHandler.Handlers
			}
			locker := &standardLockerClient{client: redislock.New(e.registry.redisServers[val].client)}
			e.locks[key] = &Locker{locker: locker, code: val, log: r.log, logHandler: logHandler}
		}
	}
	return e
}

func (r *validatedRegistry) AddLogger(handler log.Handler) {
	r.logHandler.Handlers = append(r.logHandler.Handlers, handler)
}

func (r *validatedRegistry) SetLogLevel(level log.Level) {
	logger := log.Logger{Handler: r.logHandler, Level: level}
	r.log = logger.WithField("source", "orm")
	r.log.Level = level
}

func (r *validatedRegistry) EnableDebug() {
	r.AddLogger(text.New(os.Stdout))
	r.SetLogLevel(log.DebugLevel)
}

func (r *validatedRegistry) GetTableSchema(entityName string) TableSchema {
	t, has := r.entities[entityName]
	if !has {
		return nil
	}
	return getTableSchema(r, t)
}

func (r *validatedRegistry) GetTableSchemaForEntity(entity Entity) TableSchema {
	t := reflect.TypeOf(entity)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	tableSchema := getTableSchema(r, t)
	if tableSchema == nil {
		panic(EntityNotRegisteredError{Name: t.String()})
	}
	return tableSchema
}

func (r *validatedRegistry) GetLazQueueSender() QueueSender {
	return r.lazyQueue
}

func (r *validatedRegistry) GetDirtyQueueCodes() []string {
	codes := make([]string, len(r.dirtyQueues))
	i := 0
	for code := range r.dirtyQueues {
		codes[i] = code
		i++
	}
	return codes
}

func (r *validatedRegistry) GetEntitiesForDirtyQueue(queueCode string) []string {
	results := make([]string, 0)
	if r.entities != nil {
	Exit:
		for name, t := range r.entities {
			schema := getTableSchema(r, t)
			for _, tags := range schema.tags {
				queues, has := tags["dirty"]
				if has {
					queueNames := strings.Split(queues, ",")
					for _, queueName := range queueNames {
						if queueCode == queueName {
							results = append(results, name)
							continue Exit
						}
					}
				}
			}
		}
	}
	return results
}

func (r *validatedRegistry) GetLogQueueSenders() map[string]QueueSender {
	return r.logQueues
}
