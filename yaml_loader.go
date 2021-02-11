package orm

import (
	"fmt"
	"strconv"
	"strings"
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
			case "sentinel":
				validateSentinel(registry, value, key)
			case "streams":
				validateStreams(registry, value, key)
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
		panic(fmt.Errorf("mysql uri '%v' is not valid", value))
	}
	registry.RegisterMySQLPool(asString, key)
}

func validateElasticURI(registry *Registry, value interface{}, key string, withTrace bool) {
	asString, ok := value.(string)
	if !ok {
		panic(fmt.Errorf("elastic uri '%v' is not valid", value))
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
		panic(fmt.Errorf("click house uri '%v' is not valid", value))
	}
	registry.RegisterClickHouse(asString, key)
}

func validateStreams(registry *Registry, value interface{}, key string) {
	def := fixYamlMap(value, key)
	for name, groups := range def {
		asSlice, ok := groups.([]interface{})
		if !ok {
			panic(fmt.Errorf("streams '%v' is not valid", groups))
		}
		asString := make([]string, len(asSlice))
		for i, val := range asSlice {
			asString[i] = fmt.Sprintf("%v", val)
		}
		registry.RegisterRedisStream(name, key, asString)
	}
}

func validateRedisURI(registry *Registry, value interface{}, key string) {
	asString, ok := value.(string)
	if !ok {
		panic(fmt.Errorf("redis uri '%v' is not valid", value))
	}
	elements := strings.Split(asString, ":")
	if len(elements) != 3 {
		panic(fmt.Errorf("redis uri '%v' is not valid", value))
	}
	db, err := strconv.ParseUint(elements[2], 10, 64)
	if err != nil {
		panic(fmt.Errorf("redis uri '%v' is not valid", value))
	}
	uri := fmt.Sprintf("%s:%s", elements[0], elements[1])
	registry.RegisterRedis(uri, int(db), key)
}

func validateSentinel(registry *Registry, value interface{}, key string) {
	def := fixYamlMap(value, key)
	for master, values := range def {
		asSlice, ok := values.([]interface{})
		if !ok {
			panic(fmt.Errorf("sentinel '%v' is not valid", value))
		}
		asStrings := make([]string, len(asSlice))
		for i, v := range asSlice {
			asStrings[i] = fmt.Sprintf("%v", v)
		}
		db := 0
		elements := strings.Split(master, ":")
		if len(elements) == 2 {
			master = elements[0]
			nr, err := strconv.ParseUint(elements[1], 10, 64)
			if err != nil {
				panic(fmt.Errorf("sentinel db '%v' is not valid", value))
			}
			db = int(nr)
		}
		registry.RegisterRedisSentinel(master, db, asStrings, key)
	}
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
			panic(fmt.Errorf("orm yaml key %s is not valid", key))
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
		panic(fmt.Errorf("rabbitMQ server definition '%s' not found", key))
	}
	poolName, ok := value.(string)
	if !ok {
		panic(fmt.Errorf("rabbitMQ server definition '%s' is not valid", key))
	}
	registry.RegisterRabbitMQServer(poolName, key)
	value, has = def["queues"]
	if has {
		asSlice, ok := value.([]interface{})
		if !ok {
			panic(fmt.Errorf("rabbitMQ queues definition '%s' is not valid", key))
		}
		for _, channel := range asSlice {
			asMap := fixYamlMap(channel, key)
			name, has := asMap["name"]
			if !has {
				panic(fmt.Errorf("rabbitMQ channel name '%s' not found", key))
			}
			asString, ok := name.(string)
			if !ok {
				panic(fmt.Errorf("rabbitMQ channel name '%s' is not valid", key))
			}
			durable := getBoolOptional(asMap, "durable", true)
			autoDeleted := getBoolOptional(asMap, "autodelete", false)
			ttl := getIntOptional(asMap, "ttl", 0)
			router := ""
			routerVal, has := asMap["router"]
			if has {
				asString, ok := routerVal.(string)
				if !ok {
					panic(fmt.Errorf("rabbitMQ router name '%s' is not valid", key))
				}
				router = asString
			}
			routerKeys := make([]string, 0)
			routerVal, has = asMap["router_keys"]
			if has {
				asSlice, ok := routerVal.([]interface{})
				if !ok {
					panic(fmt.Errorf("rabbitMQ router keys '%s' is not valid", key))
				}
				for _, val := range asSlice {
					asString, ok := val.(string)
					if !ok {
						panic(fmt.Errorf("rabbitMQ router key '%s' is not valid", key))
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
			panic(fmt.Errorf("rabbitMQ routers definition `%s` is not valid", key))
		}
		for _, router := range asSlice {
			asMap := fixYamlMap(router, key)
			value, has := asMap["name"]
			if !has {
				panic(fmt.Errorf("rabbitMQ router name '%s' not found", key))
			}
			nameAsString, ok := value.(string)
			if !ok {
				panic(fmt.Errorf("rabbitMQ router name '%s' is not valid", key))
			}
			value, has = asMap["type"]
			if !has {
				panic(fmt.Errorf("rabbitMQ router type '%s' not found", key))
			}
			typeAsString, ok := value.(string)
			if !ok {
				panic(fmt.Errorf("rabbitMQ router type '%s' is not valid", key))
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
		panic(fmt.Errorf("orm value for %s: %v is not valid", key, value))
	}
	return asInt
}

func validateOrmString(value interface{}, key string) string {
	asString, ok := value.(string)
	if !ok {
		panic(fmt.Errorf("orm value for %s: %v is not valid", key, value))
	}
	return asString
}
