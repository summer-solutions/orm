package orm

import (
	"fmt"
	"strconv"
	"strings"
)

func InitByYaml(yaml map[interface{}]interface{}) error {
	configData, has := yaml["orm"]
	if !has {
		return fmt.Errorf("missing orm section in config")
	}
	asMap, ok := configData.(map[interface{}]interface{})
	if !ok {
		return fmt.Errorf("invalid orm section in config")
	}
	for key, data := range asMap {
		dataAsMap, ok := data.(map[interface{}]interface{})
		if !ok {
			return fmt.Errorf("invalid orm section in config")
		}
		keyAsString, ok := key.(string)
		if !ok {
			return fmt.Errorf("invalid orm section in config")
		}
		for dataKey, value := range dataAsMap {
			switch dataKey {
			case "mysql":
				err := validateOrmMysqlUri(value, keyAsString)
				if err != nil {
					return err
				}
			case "redis":
				err := validateRedisUri(value, keyAsString)
				if err != nil {
					return err
				}
			case "redisQueues":
				poolName := keyAsString + "_queue"
				err := validateRedisUri(value, poolName)
				if err != nil {
					return err
				}
				SetRedisForQueue(poolName)
			case "localCache":
				number, err := validateOrmInt(value, keyAsString)
				if err != nil {
					return err
				}
				RegisterLocalCache(number, keyAsString)
			case "contextCache":
				number, err := validateOrmInt(value, keyAsString)
				if err != nil {
					return err
				}
				EnableContextCache(number, 1)
			default:
				return fmt.Errorf("invalid key %s in orm section", dataKey)
			}
		}
	}
	return nil
}

func validateOrmMysqlUri(value interface{}, key string) error {
	asString, ok := value.(string)
	if !ok {
		return fmt.Errorf("invalid mysql uri: %v", value)
	}
	RegisterMySqlPool(asString, key)
	var row string
	err := GetMysql(key).QueryRow("SELECT 1").Scan(&row)
	if err != nil {
		return fmt.Errorf("mysql connetion error (%s): %s", key, err.Error())
	}
	return nil
}

func validateRedisUri(value interface{}, key string) error {
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
	RegisterRedis(uri, int(db), key)
	err = GetRedis(key).Set("_spring_test_key", "1", 1)
	if err != nil {
		return fmt.Errorf("redis error (%s): %s", key, err.Error())
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
