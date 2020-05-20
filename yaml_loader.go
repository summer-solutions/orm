package orm

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/juju/errors"
)

func InitByYaml(yaml map[string]interface{}) (registry *Registry, err error) {
	registry = &Registry{}
	for key, data := range yaml {
		dataAsMap, ok := data.(map[interface{}]interface{})
		if !ok {
			return nil, errors.Errorf("invalid orm section in config")
		}
		for dataKey, value := range dataAsMap {
			switch dataKey {
			case "mysql":
				err := validateOrmMysqlURI(registry, value, key)
				if err != nil {
					return nil, errors.Trace(err)
				}
			case "redis":
				err := validateRedisURI(registry, value, key)
				if err != nil {
					return nil, errors.Trace(err)
				}
			case "rabbitMQ":
				err := validateOrmRabbitMQ(registry, value, key)
				if err != nil {
					return nil, errors.Trace(err)
				}
			case "locker":
				valAsString, err := validateOrmString(value, key)
				if err != nil {
					return nil, errors.Trace(err)
				}
				registry.RegisterLocker(key, valAsString)
			case "dirtyQueues":
				def, ok := value.(map[interface{}]interface{})
				if !ok {
					return nil, errors.Errorf("invalid dirtyQueues definition: %s", value)
				}
				for k, v := range def {
					asInt, ok := v.(int)
					if !ok {
						return nil, errors.Errorf("invalid dirtyQueues definition: %s", value)
					}
					registry.RegisterDirtyQueue(k.(string), asInt)
				}
			case "localCache":
				number, err := validateOrmInt(value, key)
				if err != nil {
					return nil, errors.Trace(err)
				}
				registry.RegisterLocalCache(number, key)
			}
		}
	}
	return registry, nil
}

func validateOrmMysqlURI(registry *Registry, value interface{}, key string) error {
	asString, ok := value.(string)
	if !ok {
		return errors.Errorf("invalid mysql uri: %v", value)
	}
	registry.RegisterMySQLPool(asString, key)
	return nil
}

func validateRedisURI(registry *Registry, value interface{}, key string) error {
	asString, ok := value.(string)
	if !ok {
		return errors.Errorf("invalid redis uri: %v", value)
	}
	elements := strings.Split(asString, ":")
	if len(elements) != 3 {
		return errors.Errorf("invalid redis uri: %v", value)
	}
	db, err := strconv.ParseUint(elements[2], 10, 64)
	if err != nil {
		return errors.Errorf("invalid redis DB id: %v", value)
	}
	uri := fmt.Sprintf("%s:%s", elements[0], elements[1])
	registry.RegisterRedis(uri, int(db), key)
	return nil
}

func getBoolOptional(data map[interface{}]interface{}, key string, defaultValue bool) bool {
	val, has := data[key]
	if !has {
		return defaultValue
	}
	return val == "true"
}

func validateOrmRabbitMQ(registry *Registry, value interface{}, key string) error {
	def, ok := value.(map[interface{}]interface{})
	if !ok {
		return errors.Errorf("invalid rabbitMQ definition: %s", key)
	}
	value, has := def["server"]
	if !has {
		return errors.Errorf("missing rabbitMQ server definition: %s", key)
	}
	poolName, ok := value.(string)
	if !ok {
		return errors.Errorf("invalid rabbitMQ server definition: %s", key)
	}
	registry.RegisterRabbitMQServer(poolName, key)
	value, has = def["queues"]
	if has {
		asSlice, ok := value.([]interface{})
		if !ok {
			return errors.Errorf("invalid rabbitMQ queues definition: %s", key)
		}
		for _, channel := range asSlice {
			asMap, ok := channel.(map[interface{}]interface{})
			if !ok {
				return errors.Errorf("invalid rabbitMQ queues definition: %s", key)
			}
			name, has := asMap["name"]
			if !has {
				return errors.Errorf("missing rabbitMQ channel name: %s", key)
			}
			asString, ok := name.(string)
			if !ok {
				return errors.Errorf("invalid rabbitMQ channel name: %s", key)
			}
			delayed := getBoolOptional(asMap, "delayed", false)
			durable := getBoolOptional(asMap, "durable", true)
			autoDeleted := getBoolOptional(asMap, "autodelete", false)
			router := ""
			routerVal, has := asMap["router"]
			if has {
				asString, ok := routerVal.(string)
				if !ok {
					return errors.Errorf("invalid rabbitMQ router name: %s", key)
				}
				router = asString
			}
			routerKeys := make([]string, 0)
			routerVal, has = asMap["router_keys"]
			if has {
				asSlice, ok := routerVal.([]interface{})
				if !ok {
					return errors.Errorf("invalid rabbitMQ router keys: %s", key)
				}
				for _, val := range asSlice {
					asString, ok := val.(string)
					if !ok {
						return errors.Errorf("invalid rabbitMQ router key: %s", key)
					}
					routerKeys = append(routerKeys, asString)
				}
			}
			prefetchCount, _ := strconv.ParseInt(fmt.Sprintf("%v", asMap["prefetchCount"]), 10, 64)
			config := &RabbitMQQueueConfig{asString, int(prefetchCount), delayed, router, durable,
				routerKeys, autoDeleted}
			registry.RegisterRabbitMQQueue(config, key)
		}
	}
	value, has = def["routers"]
	if has {
		asSlice, ok := value.([]interface{})
		if !ok {
			return errors.Errorf("invalid rabbitMQ routers definition: %s", key)
		}
		for _, router := range asSlice {
			asMap, ok := router.(map[interface{}]interface{})
			if !ok {
				return errors.Errorf("invalid rabbitMQ router definition: %s", key)
			}
			value, has := asMap["name"]
			if !has {
				return errors.Errorf("missing rabbitMQ router name: %s", key)
			}
			nameAsString, ok := value.(string)
			if !ok {
				return errors.Errorf("invalid rabbitMQ router name: %s", key)
			}
			value, has = asMap["type"]
			if !has {
				return errors.Errorf("missing rabbitMQ router type: %s", key)
			}
			typeAsString, ok := value.(string)
			if !ok {
				return errors.Errorf("invalid rabbitMQ router type: %s", key)
			}
			durable := getBoolOptional(asMap, "durable", true)
			config := &RabbitMQRouterConfig{nameAsString, typeAsString, durable}
			registry.RegisterRabbitMQRouter(config, key)
		}
	}
	return nil
}

func validateOrmInt(value interface{}, key string) (int, error) {
	asInt, ok := value.(int)
	if !ok {
		return 0, errors.Errorf("invalid orm value for %s: %v", key, value)
	}
	return asInt, nil
}

func validateOrmString(value interface{}, key string) (string, error) {
	asString, ok := value.(string)
	if !ok {
		return "", errors.Errorf("invalid orm value for %s: %v", key, value)
	}
	return asString, nil
}
