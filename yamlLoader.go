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
			case "lazyQueue":
				valAsString, err := validateOrmString(value, key)
				if err != nil {
					return nil, err
				}
				registry.RegisterLazyQueue(key, valAsString)
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
