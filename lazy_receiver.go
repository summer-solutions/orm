package orm

import (
	"strconv"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"
)

const lazyQueueName = "lazy_queue"

type LazyReceiver struct {
	engine          *Engine
	disableLoop     bool
	heartBeat       func()
	maxLoopDuration time.Duration
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

func (r *LazyReceiver) SetMaxLoopDuration(duration time.Duration) {
	r.maxLoopDuration = duration
}

func (r *LazyReceiver) Purge() {
	channel := r.engine.GetRabbitMQQueue(lazyQueueName)
	consumer := channel.NewConsumer("default consumer")
	consumer.Purge()
	consumer.Close()
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
	if r.maxLoopDuration > 0 {
		consumer.SetMaxLoopDuration(r.maxLoopDuration)
	}
	consumer.Consume(func(items [][]byte) {
		for _, item := range items {
			var data map[string]interface{}
			_ = jsoniter.ConfigFastest.Unmarshal(item, &data)
			ids := r.handleQueries(r.engine, data)
			r.handleClearCache(data, "cl", ids)
			r.handleClearCache(data, "cr", ids)
		}
	})
}

func (r *LazyReceiver) handleQueries(engine *Engine, validMap map[string]interface{}) []uint64 {
	queries := validMap["q"]
	validQueries := queries.([]interface{})
	ids := make([]uint64, len(validQueries))
	for i, query := range validQueries {
		validInsert := query.([]interface{})
		code := validInsert[0].(string)
		db := engine.GetMysql(code)
		sql := validInsert[1].(string)
		attributes := validInsert[2].([]interface{})
		func() {
			defer func() {
				if r := recover(); r != nil {
					err := r.(error)
					_, isDuplicatedError := err.(*DuplicatedKeyError)
					_, isForeignError := err.(*ForeignKeyError)
					if isDuplicatedError || isForeignError {
						engine.Log().Error(err, nil)
						engine.DataDog().RegisterAPMError(err)
						return
					}
					panic(err)
				}
			}()
			res := db.Exec(sql, attributes...)
			ids[i] = res.LastInsertId()
		}()
	}
	return ids
}

func (r *LazyReceiver) handleClearCache(validMap map[string]interface{}, key string, ids []uint64) {
	keys, has := validMap[key]
	if has {
		validKeys := keys.(map[string]interface{})
		for cacheCode, allKeys := range validKeys {
			validAllKeys := allKeys.([]interface{})
			stringKeys := make([]string, len(validAllKeys))
			for i, v := range validAllKeys {
				parts := strings.Split(v.(string), ":")
				l := len(parts)
				if parts[l-1] == "0" {
					parts[l-1] = strconv.FormatUint(ids[i], 10)
				}
				stringKeys[i] = strings.Join(parts, ":")
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
