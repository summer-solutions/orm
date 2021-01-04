package orm

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/juju/errors"
)

func InitByYaml(yaml map[string]interface{}) (registry *Registry) {
	registry = &Registry{}
	for key, data := range yaml {
		dataAsMap := fixYamlMap(data, "orm")
		for dataKey, value := range dataAsMap {
			switch dataKey {
			case "mysql":
				validateOrmMysqlURI(registry, value, key)
			case "elastic":
				validateElasticURI(registry, value, key, false)
			case "elastic_trace":
				validateElasticURI(registry, value, key, true)
			case "clickhouse":
				validateClickHouseURI(registry, value, key)
			case "redis":
				validateRedisURI(registry, value, key)
			case "redisChannels":
				validateRedisChannels(registry, value, key)
			case "rabbitmq":
				validateOrmRabbitMQ(registry, value, key)
			case "locker":
				valAsString := validateOrmString(value, key)
				registry.RegisterLocker(key, valAsString)
			case "mysqlEncoding":
				valAsString := validateOrmString(value, key)
				registry.SetDefaultEncoding(valAsString)
			case "local_cache":
				number := validateOrmInt(value, key)
				registry.RegisterLocalCache(number, key)
			}
		}
	}
	return registry
}

func validateOrmMysqlURI(registry *Registry, value interface{}, key string) {
	asString, ok := value.(string)
	if !ok {
		panic(errors.NotValidf("mysql uri '%v'", value))
	}
	registry.RegisterMySQLPool(asString, key)
}

func validateElasticURI(registry *Registry, value interface{}, key string, withTrace bool) {
	asString, ok := value.(string)
	if !ok {
		panic(errors.NotValidf("elastic uri '%v'", value))
	}
	if withTrace {
		registry.RegisterElasticWithTraceLog(asString, key)
	} else {
		registry.RegisterElastic(asString, key)
	}
}

func validateClickHouseURI(registry *Registry, value interface{}, key string) {
	asString, ok := value.(string)
	if !ok {
		panic(errors.NotValidf("click house uri '%v'", value))
	}
	registry.RegisterClickHouse(asString, key)
}

func validateRedisChannels(registry *Registry, value interface{}, key string) {
	def := fixYamlMap(value, key)
	for name, max := range def {
		asString := fmt.Sprintf("%v", max)
		parsedUint, err := strconv.ParseUint(asString, 10, 64)
		if err != nil {
			panic(errors.NotValidf("redis channel %s max size '%v'", name, max))
		}
		registry.RegisterRedisChannel(name, key, parsedUint)
	}
}

func validateRedisURI(registry *Registry, value interface{}, key string) {
	asString, ok := value.(string)
	if !ok {
		asStrings, ok := value.([]interface{})
		if ok {
			uris := make([]string, len(asStrings))
			db := uint64(0)
			for i, row := range asStrings {
				elements := strings.Split(row.(string), ":")
				if len(elements) < 2 {
					panic(errors.NotValidf("redis uri '%v'", row))
				}
				if len(elements) == 3 {
					dbUser, err := strconv.ParseUint(elements[2], 10, 64)
					if err != nil {
						panic(errors.NotValidf("redis uri '%v'", row))
					}
					db = dbUser
				}
				uris[i] = fmt.Sprintf("%s:%s", elements[0], elements[1])
			}
			registry.RegisterRedisRing(uris, int(db), key)
			return
		}
		panic(errors.NotValidf("redis uri '%v'", value))
	}
	elements := strings.Split(asString, ":")
	if len(elements) != 3 {
		panic(errors.NotValidf("redis uri '%v'", value))
	}
	db, err := strconv.ParseUint(elements[2], 10, 64)
	if err != nil {
		panic(errors.NotValidf("redis uri '%v'", value))
	}
	uri := fmt.Sprintf("%s:%s", elements[0], elements[1])
	registry.RegisterRedis(uri, int(db), key)
}

func getBoolOptional(data map[string]interface{}, key string, defaultValue bool) bool {
	val, has := data[key]
	if !has {
		return defaultValue
	}
	return val == true || val == "true"
}

func getIntOptional(data map[string]interface{}, key string, defaultValue int) int {
	val, has := data[key]
	if !has {
		return defaultValue
	}
	valInt, _ := strconv.Atoi(fmt.Sprintf("%v", val))
	return valInt
}

func fixYamlMap(value interface{}, key string) map[string]interface{} {
	def, ok := value.(map[string]interface{})
	if !ok {
		def2, ok := value.(map[interface{}]interface{})
		if !ok {
			panic(errors.NotValidf("orm yaml key %s", key))
		}
		def = make(map[string]interface{})
		for k, v := range def2 {
			def[fmt.Sprintf("%v", k)] = v
		}
	}
	return def
}

func validateOrmRabbitMQ(registry *Registry, value interface{}, key string) {
	def := fixYamlMap(value, key)
	value, has := def["server"]
	if !has {
		panic(errors.NotFoundf("rabbitMQ server definition '%s'", key))
	}
	poolName, ok := value.(string)
	if !ok {
		panic(errors.NotValidf("rabbitMQ server definition '%s'", key))
	}
	registry.RegisterRabbitMQServer(poolName, key)
	value, has = def["queues"]
	if has {
		asSlice, ok := value.([]interface{})
		if !ok {
			panic(errors.NotValidf("rabbitMQ queues definition '%s'", key))
		}
		for _, channel := range asSlice {
			asMap := fixYamlMap(channel, key)
			name, has := asMap["name"]
			if !has {
				panic(errors.NotFoundf("rabbitMQ channel name '%s'", key))
			}
			asString, ok := name.(string)
			if !ok {
				panic(errors.NotValidf("rabbitMQ channel name '%s'", key))
			}
			durable := getBoolOptional(asMap, "durable", true)
			autoDeleted := getBoolOptional(asMap, "autodelete", false)
			ttl := getIntOptional(asMap, "ttl", 0)
			router := ""
			routerVal, has := asMap["router"]
			if has {
				asString, ok := routerVal.(string)
				if !ok {
					panic(errors.NotValidf("rabbitMQ router name '%s'", key))
				}
				router = asString
			}
			routerKeys := make([]string, 0)
			routerVal, has = asMap["router_keys"]
			if has {
				asSlice, ok := routerVal.([]interface{})
				if !ok {
					panic(errors.NotValidf("rabbitMQ router keys '%s'", key))
				}
				for _, val := range asSlice {
					asString, ok := val.(string)
					if !ok {
						panic(errors.NotValidf("rabbitMQ router key '%s'", key))
					}
					routerKeys = append(routerKeys, asString)
				}
			}
			prefetchCount, _ := strconv.ParseInt(fmt.Sprintf("%v", asMap["prefetchCount"]), 10, 64)
			config := &RabbitMQQueueConfig{asString, int(prefetchCount), router, durable,
				routerKeys, autoDeleted, ttl}
			registry.RegisterRabbitMQQueue(config, key)
		}
	}
	value, has = def["routers"]
	if has {
		asSlice, ok := value.([]interface{})
		if !ok {
			panic(errors.NotValidf("rabbitMQ routers definition `%s`", key))
		}
		for _, router := range asSlice {
			asMap := fixYamlMap(router, key)
			value, has := asMap["name"]
			if !has {
				panic(errors.NotFoundf("rabbitMQ router name '%s'", key))
			}
			nameAsString, ok := value.(string)
			if !ok {
				panic(errors.NotValidf("rabbitMQ router name '%s'", key))
			}
			value, has = asMap["type"]
			if !has {
				panic(errors.NotFoundf("rabbitMQ router type '%s'", key))
			}
			typeAsString, ok := value.(string)
			if !ok {
				panic(errors.NotValidf("rabbitMQ router type '%s'", key))
			}
			durable := getBoolOptional(asMap, "durable", true)
			config := &RabbitMQRouterConfig{nameAsString, typeAsString, durable}
			registry.RegisterRabbitMQRouter(config, key)
		}
	}
}

func validateOrmInt(value interface{}, key string) int {
	asInt, ok := value.(int)
	if !ok {
		panic(errors.NotValidf("orm value for %s: %v", key, value))
	}
	return asInt
}

func validateOrmString(value interface{}, key string) string {
	asString, ok := value.(string)
	if !ok {
		panic(errors.NotValidf("orm value for %s: %v", key, value))
	}
	return asString
}
