package orm

import (
	"encoding/json"
	"fmt"
)

type LazyReceiver struct {
	engine    *Engine
	queueName string
}

func NewLazyReceiver(engine *Engine, queueName string) *LazyReceiver {
	return &LazyReceiver{engine: engine, queueName: queueName}
}

func (r *LazyReceiver) Size() (int64, error) {
	code := r.queueName + "_queue"
	redis, has := r.engine.GetRedis(code)
	if !has {
		return 0, RedisCachePoolNotRegisteredError{Name: code}
	}
	return redis.LLen("lazy_queue")
}

func (r *LazyReceiver) Digest() (has bool, err error) {
	code := r.queueName + "_queue"
	redis, has := r.engine.GetRedis(code)
	if !has {
		return false, RedisCachePoolNotRegisteredError{Name: code}
	}
	key := "lazy_queue"
	val, found, err := redis.RPop(key)
	if err != nil {
		return false, err
	}
	if !found {
		return false, nil
	}
	var data interface{}
	err = json.Unmarshal([]byte(val), &data)
	if err != nil {
		return true, err
	}
	brokenMap := make(map[string]interface{})
	validMap, ok := data.(map[string]interface{})
	if !ok {
		return true, fmt.Errorf("invalid map: %v", data)
	}
	err = r.handleQueries(r.engine, validMap, brokenMap)
	if err != nil {
		return true, err
	}
	err = r.handleClearCache(validMap, brokenMap, "cl")
	if err != nil {
		return true, err
	}
	err = r.handleClearCache(validMap, brokenMap, "cr")
	if err != nil {
		return true, err
	}
	if len(brokenMap) > 0 {
		v, err := serializeForLazyQueue(brokenMap)
		if err != nil {
			return true, err
		}
		code := "default"
		redis, has := r.engine.getRedisForQueue(code)
		if !has {
			return false, RedisCachePoolNotRegisteredError{Name: code}
		}
		_, err = redis.RPush("lazy_queue", v)
		if err != nil {
			return true, err
		}
	}
	return true, nil
}

func (r *LazyReceiver) handleQueries(engine *Engine, validMap map[string]interface{}, brokenMap map[string]interface{}) error {
	queries, has := validMap["q"]
	if has {
		validQueries, ok := queries.([]interface{})
		if !ok {
			return fmt.Errorf("invalid queries: %v", queries)
		}
		for _, query := range validQueries {
			validInsert, ok := query.([]interface{})
			if !ok {
				return fmt.Errorf("invalid query: %v", validInsert)
			}
			code := validInsert[0].(string)
			db, has := engine.GetMysql(code)
			if !has {
				return DBPoolNotRegisteredError{Name: code}
			}
			sql := validInsert[1].(string)
			attributes := validInsert[2].([]interface{})
			_, err := db.Exec(sql, attributes...)
			if err != nil {
				brokenMap["q"] = validMap["q"]
				return err
			}
		}
	}
	return nil
}

func (r *LazyReceiver) handleClearCache(validMap map[string]interface{}, brokenMap map[string]interface{}, key string) error {
	keys, has := validMap[key]
	if has {
		validKeys, ok := keys.(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid cache keys: %v", keys)
		}
		for cacheCode, allKeys := range validKeys {
			validAllKeys, ok := allKeys.([]interface{})
			if !ok {
				return fmt.Errorf("invalid cache keys: %v", allKeys)
			}
			stringKeys := make([]string, len(validAllKeys))
			for i, v := range validAllKeys {
				stringKeys[i] = v.(string)
			}
			if key == "cl" {
				if r.engine.localCache == nil {
					return fmt.Errorf("unknown local cache %s", cacheCode)
				}
				cache, has := r.engine.localCache[cacheCode]
				if !has {
					return fmt.Errorf("unknown local cache %s", cacheCode)
				}
				cache.Remove(stringKeys...)
			} else {
				if r.engine.redis == nil {
					return fmt.Errorf("unknown redis cache %s", cacheCode)
				}
				cache, has := r.engine.redis[cacheCode]
				if !has {
					return fmt.Errorf("unknown redis cache %s", cacheCode)
				}
				err := cache.Del(stringKeys...)
				if err != nil {
					if brokenMap[key] == nil {
						brokenMap[key] = make(map[string][]interface{})
					}
					brokenMap[key].(map[string][]interface{})[cacheCode] = validAllKeys
					return err
				}
			}
		}
	}
	return nil
}
