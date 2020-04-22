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
	redis := r.engine.GetRedis(code)
	return redis.LLen("_lazy_queue")
}

func (r *LazyReceiver) Digest() (has bool, err error) {
	code := r.queueName + "_queue"
	redis := r.engine.GetRedis(code)
	key := "_lazy_queue"
	val, found, err := redis.RPop(key)
	if err != nil {
		return false, fmt.Errorf("%w", err)
	}
	if !found {
		return false, nil
	}
	var data interface{}
	err = json.Unmarshal([]byte(val), &data)
	if err != nil {
		return true, err
	}
	validMap := data.(map[string]interface{})
	err = r.handleQueries(r.engine, validMap)
	if err != nil {
		return true, err
	}
	err = r.handleClearCache(validMap, "cl")
	if err != nil {
		return true, err
	}
	err = r.handleClearCache(validMap, "cr")
	if err != nil {
		return true, err
	}
	return true, nil
}

func (r *LazyReceiver) handleQueries(engine *Engine, validMap map[string]interface{}) error {
	queries, has := validMap["q"]
	if has {
		validQueries := queries.([]interface{})
		for _, query := range validQueries {
			validInsert := query.([]interface{})
			code := validInsert[0].(string)
			db := engine.GetMysql(code)
			sql := validInsert[1].(string)
			attributes := validInsert[2].([]interface{})
			_, err := db.Exec(sql, attributes...)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (r *LazyReceiver) handleClearCache(validMap map[string]interface{}, key string) error {
	keys, has := validMap[key]
	if has {
		validKeys := keys.(map[string]interface{})
		for cacheCode, allKeys := range validKeys {
			validAllKeys := allKeys.([]interface{})
			stringKeys := make([]string, len(validAllKeys))
			for i, v := range validAllKeys {
				stringKeys[i] = v.(string)
			}
			if key == "cl" {
				cache := r.engine.localCache[cacheCode]
				cache.Remove(stringKeys...)
			} else {
				cache := r.engine.redis[cacheCode]
				err := cache.Del(stringKeys...)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
