package orm

import (
	jsoniter "github.com/json-iterator/go"
	"github.com/juju/errors"
)

const lazyQueueName = "lazy_queue"

type LazyReceiver struct {
	engine      *Engine
	disableLoop bool
}

func NewLazyReceiver(engine *Engine) *LazyReceiver {
	return &LazyReceiver{engine: engine}
}

func (r *LazyReceiver) DisableLoop() {
	r.disableLoop = true
}

func (r *LazyReceiver) Digest() error {
	channel := r.engine.GetRabbitMQQueue(lazyQueueName)
	consumer, err := channel.NewConsumer("default consumer")
	if err != nil {
		return errors.Trace(err)
	}
	defer consumer.Close()
	if r.disableLoop {
		consumer.DisableLoop()
	}
	var data interface{}
	err = consumer.Consume(func(items [][]byte) error {
		for _, item := range items {
			_ = jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(item, &data)
			validMap := data.(map[string]interface{})
			err := r.handleQueries(r.engine, validMap)
			if err != nil {
				return errors.Trace(err)
			}
			err = r.handleClearCache(validMap, "cl")
			if err != nil {
				return errors.Trace(err)
			}
			err = r.handleClearCache(validMap, "cr")
			if err != nil {
				return errors.Trace(err)
			}
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	return nil
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
				return errors.Trace(err)
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
					return errors.Trace(err)
				}
			}
		}
	}
	return nil
}
