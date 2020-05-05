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
			case "lazyQueue":
				valAsString, err := validateOrmString(value, key)
				if err != nil {
					return nil, err
				}
				registry.RegisterLazyQueue(&RedisQueueSender{PoolName: valAsString})
			case "locker":
				valAsString, err := validateOrmString(value, key)
				if err != nil {
					return nil, err
				}
				registry.RegisterLocker(key, valAsString)
			case "dirtyQueue":
				valAsString, err := validateOrmString(value, key)
				if err != nil {
					return nil, err
				}
				registry.RegisterDirtyQueue(key, &RedisDirtyQueueSender{PoolName: valAsString})
			case "logQueue":
				valAsString, err := validateOrmString(value, key)
				if err != nil {
					return nil, err
				}
				registry.RegisterLogQueue(key, &RedisQueueSender{PoolName: valAsString})
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
	value, has = def["channels"]
	if !has {
		return nil
	}
	asSlice, ok := value.([]interface{})
	if !ok {
		return fmt.Errorf("invalid rabbitMQ channels definition: %s", key)
	}
	for _, channel := range asSlice {
		asMap, ok := channel.(map[interface{}]interface{})
		if !ok {
			return fmt.Errorf("invalid rabbitMQ channels definition: %s", key)
		}
		name, has := asMap["name"]
		if !has {
			return fmt.Errorf("missing rabbitMQ channel name: %s", key)
		}
		asString, ok := name.(string)
		if !ok {
			return fmt.Errorf("invalid rabbitMQ channel name: %s", key)
		}
		passive := asMap["passive"] == true
		durrable := asMap["durrable"] == true
		exclusive := asMap["exclusive"] == true
		autodelete := asMap["autodelete"] == true
		nowait := asMap["nowait"] == true
		prefetchCount, _ := strconv.ParseInt(fmt.Sprintf("%v", asMap["prefetchCount"]), 10, 64)
		prefetchSize, _ := strconv.ParseInt(fmt.Sprintf("%v", asMap["prefetchSize"]), 10, 64)
		config := &RabbitMQChannelConfig{asString, passive, durrable,
			exclusive, autodelete, nowait, int(prefetchCount), int(prefetchSize), nil}
		registry.RegisterRabbitMQChannel(key, config)
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
