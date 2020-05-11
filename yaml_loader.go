package orm

import (
	"fmt"
	"strconv"
	"strings"
)

func InitByYaml(yaml map[string]interface{}) (registry *Registry, err error) {
	registry = &Registry{}
	for key, data := range yaml {
		dataAsMap, ok := data.(map[interface{}]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid orm section in config")
		}
		for dataKey, value := range dataAsMap {
			switch dataKey {
			case "mysql":
				err := validateOrmMysqlURI(registry, value, key)
				if err != nil {
					return nil, err
				}
			case "redis":
				err := validateRedisURI(registry, value, key)
				if err != nil {
					return nil, err
				}
			case "rabbitMQ":
				err := validateOrmRabbitMQ(registry, value, key)
				if err != nil {
					return nil, err
				}
			case "lazyQueueRedis":
				valAsString, err := validateOrmString(value, key)
				if err != nil {
					return nil, err
				}
				registry.RegisterLazyQueue(&RedisQueueSender{PoolName: valAsString})
			case "lazyQueueRabbitMQ":
				valAsString, err := validateOrmString(value, key)
				if err != nil {
					return nil, err
				}
				registry.RegisterLazyQueue(&RabbitMQQueueSender{QueueName: valAsString})
			case "locker":
				valAsString, err := validateOrmString(value, key)
				if err != nil {
					return nil, err
				}
				registry.RegisterLocker(key, valAsString)
			case "dirtyQueueRedis":
				valAsString, err := validateOrmString(value, key)
				if err != nil {
					return nil, err
				}
				registry.RegisterDirtyQueue(key, &RedisQueueSender{PoolName: valAsString})
			case "dirtyQueueRabbitMQ":
				valAsString, err := validateOrmString(value, key)
				if err != nil {
					return nil, err
				}
				registry.RegisterDirtyQueue(key, &RabbitMQQueueSender{QueueName: valAsString})
			case "logQueueRedis":
				valAsString, err := validateOrmString(value, key)
				if err != nil {
					return nil, err
				}
				registry.RegisterLogQueue(key, &RedisQueueSender{PoolName: valAsString})
			case "logQueueRabbitMQ":
				valAsString, err := validateOrmString(value, key)
				if err != nil {
					return nil, err
				}
				registry.RegisterLogQueue(key, &RabbitMQQueueSender{QueueName: valAsString})
			case "localCache":
				number, err := validateOrmInt(value, key)
				if err != nil {
					return nil, err
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
		return fmt.Errorf("invalid mysql uri: %v", value)
	}
	registry.RegisterMySQLPool(asString, key)
	return nil
}

func validateRedisURI(registry *Registry, value interface{}, key string) error {
	asString, ok := value.(string)
	if !ok {
		return fmt.Errorf("invalid redis uri: %v", value)
	}
	elements := strings.Split(asString, ":")
	if len(elements) != 3 {
		return fmt.Errorf("invalid redis uri: %v", value)
	}
	db, err := strconv.ParseUint(elements[2], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid redis DB id: %v", value)
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
		return fmt.Errorf("invalid rabbitMQ definition: %s", key)
	}
	value, has := def["server"]
	if !has {
		return fmt.Errorf("missing rabbitMQ server definition: %s", key)
	}
	poolName, ok := value.(string)
	if !ok {
		return fmt.Errorf("invalid rabbitMQ server definition: %s", key)
	}
	registry.RegisterRabbitMQServer(poolName, key)
	value, has = def["queues"]
	if has {
		asSlice, ok := value.([]interface{})
		if !ok {
			return fmt.Errorf("invalid rabbitMQ queues definition: %s", key)
		}
		for _, channel := range asSlice {
			asMap, ok := channel.(map[interface{}]interface{})
			if !ok {
				return fmt.Errorf("invalid rabbitMQ queues definition: %s", key)
			}
			name, has := asMap["name"]
			if !has {
				return fmt.Errorf("missing rabbitMQ channel name: %s", key)
			}
			asString, ok := name.(string)
			if !ok {
				return fmt.Errorf("invalid rabbitMQ channel name: %s", key)
			}
			delayed := getBoolOptional(asMap, "delayed", false)
			autoDelete := getBoolOptional(asMap, "autodelete", false)
			router := ""
			exchangeVal, has := asMap["exchange"]
			if has {
				asString, ok := exchangeVal.(string)
				if !ok {
					return fmt.Errorf("invalid rabbitMQ exchange name: %s", key)
				}
				router = asString
			}
			routerKeys := make([]string, 0)
			exchangeVal, has = asMap["router_keys"]
			if has {
				asSlice, ok := exchangeVal.([]interface{})
				if !ok {
					return fmt.Errorf("invalid rabbitMQ exchange keys: %s", key)
				}
				for _, val := range asSlice {
					asString, ok := val.(string)
					if !ok {
						return fmt.Errorf("invalid rabbitMQ exchange key: %s", key)
					}
					routerKeys = append(routerKeys, asString)
				}
			}
			prefetchCount, _ := strconv.ParseInt(fmt.Sprintf("%v", asMap["prefetchCount"]), 10, 64)
			config := &RabbitMQQueueConfig{asString, int(prefetchCount), delayed, autoDelete, router, routerKeys}
			registry.RegisterRabbitMQQueue(key, config)
		}
	}
	value, has = def["exchanges"]
	if has {
		asSlice, ok := value.([]interface{})
		if !ok {
			return fmt.Errorf("invalid rabbitMQ exchanges definition: %s", key)
		}
		for _, exchange := range asSlice {
			asMap, ok := exchange.(map[interface{}]interface{})
			if !ok {
				return fmt.Errorf("invalid rabbitMQ exchange definition: %s", key)
			}
			value, has := asMap["name"]
			if !has {
				return fmt.Errorf("missing rabbitMQ exchange name: %s", key)
			}
			nameAsString, ok := value.(string)
			if !ok {
				return fmt.Errorf("invalid rabbitMQ exchange name: %s", key)
			}
			value, has = asMap["type"]
			if !has {
				return fmt.Errorf("missing rabbitMQ exchange type: %s", key)
			}
			typeAsString, ok := value.(string)
			if !ok {
				return fmt.Errorf("invalid rabbitMQ exchange type: %s", key)
			}
			autoDelete := getBoolOptional(asMap, "autodelete", false)
			config := &RabbitMQRouterConfig{nameAsString, typeAsString, autoDelete}
			registry.RegisterRabbitMQRouter(key, config)
		}
	}
	return nil
}

func validateOrmInt(value interface{}, key string) (int, error) {
	asInt, ok := value.(int)
	if !ok {
		return 0, fmt.Errorf("invalid orm value for %s: %v", key, value)
	}
	return asInt, nil
}

func validateOrmString(value interface{}, key string) (string, error) {
	asString, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("invalid orm value for %s: %v", key, value)
	}
	return asString, nil
}
