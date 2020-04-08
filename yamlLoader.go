package orm

import (
	"fmt"
	"strconv"
	"strings"
)

func InitByYaml(yaml map[interface{}]interface{}) (registry *Registry, err error) {
	configData, has := yaml["orm"]
	if !has {
		return nil, fmt.Errorf("missing orm section in config")
	}
	asMap, ok := configData.(map[interface{}]interface{})
	if !ok {
		return nil, fmt.Errorf("invalid orm section in config")
	}
	registry = &Registry{}
	for key, data := range asMap {
		dataAsMap, ok := data.(map[interface{}]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid orm section in config")
		}
		keyAsString, ok := key.(string)
		if !ok {
			return nil, fmt.Errorf("invalid orm section in config")
		}
		for dataKey, value := range dataAsMap {
			switch dataKey {
			case "mysql":
				err := validateOrmMysqlUri(registry, value, keyAsString)
				if err != nil {
					return nil, err
				}
			case "redis":
				err := validateRedisUri(registry, value, keyAsString)
				if err != nil {
					return nil, err
				}
			case "lazyQueue":
				valAsString, err := validateOrmString(value, keyAsString)
				if err != nil {
					return nil, err
				}
				registry.RegisterLazyQueue(keyAsString, valAsString)
			case "dirtyQueue":
				valAsString, err := validateOrmString(value, keyAsString)
				if err != nil {
					return nil, err
				}
				registry.RegisterDirtyQueue(keyAsString, &RedisDirtyQueueSender{PoolName: valAsString})
			case "localCache":
				number, err := validateOrmInt(value, keyAsString)
				if err != nil {
					return nil, err
				}
				registry.RegisterLocalCache(number, keyAsString)
			default:
				return nil, fmt.Errorf("invalid key %s in orm section", dataKey)
			}
		}
	}
	return registry, nil
}

func validateOrmMysqlUri(registry *Registry, value interface{}, key string) error {
	asString, ok := value.(string)
	if !ok {
		return fmt.Errorf("invalid mysql uri: %v", value)
	}
	registry.RegisterMySqlPool(asString, key)
	return nil
}

func validateRedisUri(registry *Registry, value interface{}, key string) error {
	asString, ok := value.(string)
	if !ok {
		return fmt.Errorf("invalid mysql uri: %v", value)
	}
	elements := strings.Split(asString, ":")
	db, err := strconv.ParseUint(elements[2], 10, 64)
	if err != nil {
		return fmt.Errorf("invalid redis uri: %v", value)
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
