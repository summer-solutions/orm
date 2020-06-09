package orm

import (
	jsoniter "github.com/json-iterator/go"
)

const lazyQueueName = "lazy_queue"

type LazyReceiver struct {
	engine      *Engine
	disableLoop bool
	heartBeat   func()
}

func NewLazyReceiver(engine *Engine) *LazyReceiver {
	return &LazyReceiver{engine: engine}
}

func (r *LazyReceiver) DisableLoop() {
	r.disableLoop = true
}

func (r *LazyReceiver) SetHeartBeat(beat func()) {
	r.heartBeat = beat
}

func (r *LazyReceiver) Digest() {
	channel := r.engine.GetRabbitMQQueue(lazyQueueName)
	consumer := channel.NewConsumer("default consumer")
	defer consumer.Close()
	if r.disableLoop {
		consumer.DisableLoop()
	}
	if r.heartBeat != nil {
		consumer.SetHeartBeat(r.heartBeat)
	}
	var data interface{}
	consumer.Consume(func(items [][]byte) {
		for _, item := range items {
			_ = jsoniter.ConfigFastest.Unmarshal(item, &data)
			validMap := data.(map[string]interface{})
			r.handleQueries(r.engine, validMap)
			r.handleClearCache(validMap, "cl")
			r.handleClearCache(validMap, "cr")
		}
	})
}

func (r *LazyReceiver) handleQueries(engine *Engine, validMap map[string]interface{}) {
	queries, has := validMap["q"]
	if has {
		validQueries := queries.([]interface{})
		for _, query := range validQueries {
			validInsert := query.([]interface{})
			code := validInsert[0].(string)
			db := engine.GetMysql(code)
			sql := validInsert[1].(string)
			attributes := validInsert[2].([]interface{})
			_ = db.Exec(sql, attributes...)
		}
	}
}

func (r *LazyReceiver) handleClearCache(validMap map[string]interface{}, key string) {
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
				cache.Del(stringKeys...)
			}
		}
	}
}
