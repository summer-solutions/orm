package orm

import (
	"database/sql"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
	"github.com/golang/groupcache/lru"
	"github.com/juju/errors"
	"github.com/olivere/elastic/v7"
)

type Registry struct {
	sqlClients           map[string]*DBConfig
	localCacheContainers map[string]*LocalCacheConfig
	redisServers         map[string]*RedisCacheConfig
	elasticServers       map[string]*ElasticConfig
	rabbitMQServers      map[string]*rabbitMQConfig
	rabbitMQQueues       map[string][]*RabbitMQQueueConfig
	rabbitMQRouters      map[string][]*RabbitMQRouterConfig
	entities             map[string]reflect.Type
	enums                map[string]Enum
	dirtyQueues          map[string]int
	locks                map[string]string
}

func (r *Registry) Validate() (ValidatedRegistry, error) {
	registry := &validatedRegistry{}
	l := len(r.entities)
	registry.tableSchemas = make(map[reflect.Type]*tableSchema, l)
	registry.entities = make(map[string]reflect.Type)
	if registry.sqlClients == nil {
		registry.sqlClients = make(map[string]*DBConfig)
	}
	for k, v := range r.sqlClients {
		db, err := sql.Open("mysql", v.dataSourceName)
		if err != nil {
			return nil, errors.Trace(err)
		}
		var maxConnections int
		var skip string
		err = db.QueryRow("SHOW VARIABLES LIKE 'max_connections'").Scan(&skip, &maxConnections)
		if err != nil {
			return nil, errors.Annotatef(err, "can't connect to mysql '%s'", v.code)
		}
		var waitTimeout int
		err = db.QueryRow("SHOW VARIABLES LIKE 'wait_timeout'").Scan(&skip, &waitTimeout)
		if err != nil {
			return nil, errors.Trace(err)
		}
		maxConnections = int(math.Floor(float64(maxConnections) * 0.9))
		if maxConnections == 0 {
			maxConnections = 1
		}
		maxIdleConnections := int(math.Floor(float64(maxConnections) * 0.2))
		if maxIdleConnections == 0 {
			maxIdleConnections = 2
		}
		waitTimeout = int(math.Floor(float64(waitTimeout) * 0.8))
		if waitTimeout == 0 {
			waitTimeout = 1
		}
		db.SetMaxOpenConns(maxConnections)
		db.SetMaxIdleConns(maxIdleConnections)
		db.SetConnMaxLifetime(time.Duration(waitTimeout) * time.Second)
		v.db = db
		registry.sqlClients[k] = v
	}
	if registry.dirtyQueues == nil {
		registry.dirtyQueues = make(map[string]int)
	}
	for k, v := range r.dirtyQueues {
		registry.dirtyQueues[k] = v
	}
	if registry.lockServers == nil {
		registry.lockServers = make(map[string]string)
	}
	for k, v := range r.locks {
		registry.lockServers[k] = v
	}

	if registry.localCacheContainers == nil {
		registry.localCacheContainers = make(map[string]*LocalCacheConfig)
	}
	for k, v := range r.localCacheContainers {
		registry.localCacheContainers[k] = v
	}
	if registry.redisServers == nil {
		registry.redisServers = make(map[string]*RedisCacheConfig)
	}
	for k, v := range r.redisServers {
		registry.redisServers[k] = v
	}

	if registry.elasticServers == nil {
		registry.elasticServers = make(map[string]*ElasticConfig)
	}
	for k, v := range r.elasticServers {
		registry.elasticServers[k] = v
	}

	if registry.rabbitMQServers == nil {
		registry.rabbitMQServers = make(map[string]*rabbitMQConnection)
	}
	for k, v := range r.rabbitMQServers {
		rConn := &rabbitMQConnection{config: v}
		registry.rabbitMQServers[k] = rConn
	}
	if registry.rabbitMQRouterConfigs == nil {
		registry.rabbitMQRouterConfigs = make(map[string]*RabbitMQRouterConfig)
	}
	for connectionCode, routers := range r.rabbitMQRouters {
		_, has := registry.rabbitMQServers[connectionCode]
		if !has {
			return nil, errors.Errorf("rabbitMQ server '%s' is not registered", connectionCode)
		}
		for _, def := range routers {
			_, has := registry.rabbitMQRouterConfigs[def.Name]
			if has {
				return nil, errors.Errorf("rabbitMQ router name '%s' already exists", def.Name)
			}
			registry.rabbitMQRouterConfigs[def.Name] = def
		}
	}

	if registry.rabbitMQChannelsToQueue == nil {
		registry.rabbitMQChannelsToQueue = make(map[string]*rabbitMQChannelToQueue)
	}
	for connectionCode, queues := range r.rabbitMQQueues {
		connection, has := registry.rabbitMQServers[connectionCode]
		if !has {
			return nil, errors.Errorf("rabbitMQ server '%s' is not registered", connectionCode)
		}
		for _, def := range queues {
			_, has := registry.rabbitMQChannelsToQueue[def.Name]
			if has {
				return nil, errors.Errorf("rabbitMQ channel name '%s' already exists", def.Name)
			}
			if def.Router != "" {
				_, has := registry.rabbitMQRouterConfigs[def.Router]
				if !has {
					return nil, errors.Errorf("rabbitMQ router name '%s' is not registered", def.Router)
				}
			}
			if def.Delayed && def.Router == "" {
				if registry.rabbitMQRouterConfigs == nil {
					registry.rabbitMQRouterConfigs = make(map[string]*RabbitMQRouterConfig)
				}
				registry.rabbitMQRouterConfigs[def.Name] = &RabbitMQRouterConfig{Name: def.Name + "_router", Type: "direct"}
			}
			channel := &rabbitMQChannelToQueue{connection: connection, config: def}
			registry.rabbitMQChannelsToQueue[def.Name] = channel
		}
	}
	if registry.enums == nil {
		registry.enums = make(map[string]Enum)
	}
	for k, v := range r.enums {
		registry.enums[k] = v
	}
	for name, entityType := range r.entities {
		tableSchema, err := initTableSchema(r, entityType)
		if err != nil {
			return nil, err
		}
		registry.tableSchemas[entityType] = tableSchema
		registry.entities[name] = entityType
	}
	engine := registry.CreateEngine()
	hasLog := false
	for _, schema := range registry.tableSchemas {
		_, err := checkStruct(schema, engine, schema.t, make(map[string]*index), make(map[string]*foreignIndex), "")
		if err != nil {
			return nil, errors.Annotatef(err, "invalid entity struct '%s'", schema.t.String())
		}
		if schema.hasLog {
			hasLog = true
		}
	}
	if hasLog && registry.rabbitMQChannelsToQueue[logQueueName] == nil {
		connection, has := registry.rabbitMQServers["default"]
		if !has {
			return nil, errors.Errorf("missing default rabbitMQ connection to handle entity change log")
		}
		def := &RabbitMQQueueConfig{Name: logQueueName, Durable: true}
		registry.rabbitMQChannelsToQueue[logQueueName] = &rabbitMQChannelToQueue{connection: connection, config: def}
	}
	if registry.rabbitMQChannelsToQueue[lazyQueueName] == nil {
		connection, has := registry.rabbitMQServers["default"]
		if !has {
			return nil, errors.Errorf("missing default rabbitMQ connection to handle lazyFlush")
		}
		def := &RabbitMQQueueConfig{Name: lazyQueueName, Durable: true}
		registry.rabbitMQChannelsToQueue[lazyQueueName] = &rabbitMQChannelToQueue{connection: connection, config: def}
	}
	if registry.rabbitMQChannelsToQueue[flushCacheQueueName] == nil {
		connection, has := registry.rabbitMQServers["default"]
		if !has {
			return nil, errors.Errorf("missing default rabbitMQ connection to handle flushInCache")
		}
		def := &RabbitMQQueueConfig{Name: flushCacheQueueName, Durable: true}
		registry.rabbitMQChannelsToQueue[flushCacheQueueName] = &rabbitMQChannelToQueue{connection: connection, config: def}
	}
	queues := registry.GetDirtyQueues()
	if len(queues) > 0 {
		connection, has := registry.rabbitMQServers["default"]
		if !has {
			return nil, errors.Errorf("missing default rabbitMQ connection to handle flushInCache")
		}
		for name, max := range registry.GetDirtyQueues() {
			queueName := "dirty_queue_" + name
			def := &RabbitMQQueueConfig{Name: queueName, Durable: false, PrefetchCount: max}
			registry.rabbitMQChannelsToQueue[queueName] = &rabbitMQChannelToQueue{connection: connection, config: def}
		}
	}
	//init rabbitMQ channels
	engine = registry.CreateEngine()
	for code, config := range registry.rabbitMQChannelsToQueue {
		if config.config.Router == "" {
			if config.config.Delayed {
				r := engine.GetRabbitMQDelayedQueue(code)
				receiver := r.initChannel(config.config.Name, false)
				err := receiver.Close()
				if err != nil {
					panic(err)
				}
			} else {
				r := engine.GetRabbitMQQueue(code)
				receiver := r.initChannel(config.config.Name, false)
				err := receiver.Close()
				if err != nil {
					panic(err)
				}
			}
		} else {
			r := engine.GetRabbitMQRouter(code)
			receiver := r.initChannel(config.config.Name, false)
			err := receiver.Close()
			if err != nil {
				panic(err)
			}
		}
	}
	return registry, nil
}

func (r *Registry) RegisterEntity(entity ...interface{}) {
	if r.entities == nil {
		r.entities = make(map[string]reflect.Type)
	}
	for _, e := range entity {
		t := reflect.TypeOf(e)
		if t.Kind() == reflect.Ptr {
			t = t.Elem()
		}
		r.entities[t.String()] = t
	}
}

func (r *Registry) RegisterEnumStruct(code string, val Enum) {
	val.init(val)
	if r.enums == nil {
		r.enums = make(map[string]Enum)
	}
	r.enums[code] = val
}

func (r *Registry) RegisterEnumSlice(code string, val []string) {
	e := EnumModel{}
	e.fields = val
	e.defaultValue = val[0]
	e.mapping = make(map[string]string)
	for _, name := range val {
		e.mapping[name] = name
	}
	if r.enums == nil {
		r.enums = make(map[string]Enum)
	}
	r.enums[code] = &e
}

func (r *Registry) RegisterEnumMap(code string, val map[string]string, defaultValue string) {
	e := EnumModel{}
	e.mapping = val
	e.defaultValue = defaultValue
	fields := make([]string, 0)
	for name := range val {
		fields = append(fields, name)
	}
	sort.Strings(fields)
	e.fields = fields
	if r.enums == nil {
		r.enums = make(map[string]Enum)
	}
	r.enums[code] = &e
}

func (r *Registry) RegisterMySQLPool(dataSourceName string, code ...string) {
	r.registerSQLPool(dataSourceName, code...)
}

func (r *Registry) RegisterElasticPool(url string, code ...string) {
	r.RegisterElastic(url, code...)
}

func (r *Registry) RegisterLocalCache(size int, code ...string) {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	if r.localCacheContainers == nil {
		r.localCacheContainers = make(map[string]*LocalCacheConfig)
	}
	r.localCacheContainers[dbCode] = &LocalCacheConfig{code: dbCode, lru: lru.New(size)}
}

func (r *Registry) RegisterRedis(address string, db int, code ...string) {
	client := redis.NewClient(&redis.Options{
		Addr: address,
		DB:   db,
	})
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	redisCache := &RedisCacheConfig{code: dbCode, client: client}
	if r.redisServers == nil {
		r.redisServers = make(map[string]*RedisCacheConfig)
	}
	r.redisServers[dbCode] = redisCache
}

func (r *Registry) RegisterRabbitMQServer(address string, code ...string) {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	rabbitMQ := &rabbitMQConfig{code: dbCode, address: address}
	if r.rabbitMQServers == nil {
		r.rabbitMQServers = make(map[string]*rabbitMQConfig)
	}
	r.rabbitMQServers[dbCode] = rabbitMQ
}

func (r *Registry) RegisterRabbitMQQueue(config *RabbitMQQueueConfig, serverPool ...string) {
	dbCode := "default"
	if len(serverPool) > 0 {
		dbCode = serverPool[0]
	}
	if r.rabbitMQQueues == nil {
		r.rabbitMQQueues = make(map[string][]*RabbitMQQueueConfig)
	}
	if r.rabbitMQQueues[dbCode] == nil {
		r.rabbitMQQueues[dbCode] = make([]*RabbitMQQueueConfig, 0)
	}
	r.rabbitMQQueues[dbCode] = append(r.rabbitMQQueues[dbCode], config)
}

func (r *Registry) RegisterRabbitMQRouter(config *RabbitMQRouterConfig, serverPool ...string) {
	dbCode := "default"
	if len(serverPool) > 0 {
		dbCode = serverPool[0]
	}
	if r.rabbitMQRouters == nil {
		r.rabbitMQRouters = make(map[string][]*RabbitMQRouterConfig)
	}
	if r.rabbitMQRouters[dbCode] == nil {
		r.rabbitMQRouters[dbCode] = make([]*RabbitMQRouterConfig, 0)
	}
	r.rabbitMQRouters[dbCode] = append(r.rabbitMQRouters[dbCode], config)
}

func (r *Registry) RegisterDirtyQueue(code string, batchSize int) {
	if r.dirtyQueues == nil {
		r.dirtyQueues = make(map[string]int)
	}
	r.dirtyQueues[code] = batchSize
}

func (r *Registry) RegisterLocker(code string, redisCode string) {
	if r.locks == nil {
		r.locks = make(map[string]string)
	}
	r.locks[code] = redisCode
}

func (r *Registry) registerSQLPool(dataSourceName string, code ...string) {
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	db := &DBConfig{code: dbCode, dataSourceName: dataSourceName}
	if r.sqlClients == nil {
		r.sqlClients = make(map[string]*DBConfig)
	}
	parts := strings.Split(dataSourceName, "/")
	dbName := strings.Split(parts[len(parts)-1], "?")[0]

	db.databaseName = dbName
	r.sqlClients[dbCode] = db
}

func (r *Registry) RegisterElastic(url string, code ...string) {
	client, err := elastic.NewClient(
		elastic.SetSniff(false),
		elastic.SetURL(url),
		elastic.SetHealthcheckInterval(5*time.Second),
	)

	if err != nil {
		panic(err)
	}
	dbCode := "default"
	if len(code) > 0 {
		dbCode = code[0]
	}
	config := &ElasticConfig{code: dbCode, client: client}
	if r.elasticServers == nil {
		r.elasticServers = make(map[string]*ElasticConfig)
	}
	r.elasticServers[dbCode] = config
}

type RedisCacheConfig struct {
	code   string
	client *redis.Client
}

type ElasticConfig struct {
	code   string
	client *elastic.Client
}
