package orm

import (
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"

	jsoniter "github.com/json-iterator/go"
)

const lazyChannelName = "orm-lazy-channel"

type LazyReceiver struct {
	engine            *Engine
	block             time.Duration
	disableLoop       bool
	heartBeat         func()
	heartBeatDuration time.Duration
}

func NewLazyReceiver(engine *Engine) *LazyReceiver {
	return &LazyReceiver{engine: engine, block: time.Minute}
}

func (r *LazyReceiver) DisableLoop() {
	r.disableLoop = true
}

func (r *LazyReceiver) SetBlock(duration time.Duration) {
	r.block = duration
}

func (r *LazyReceiver) SetHeartBeat(duration time.Duration, beat func()) {
	r.heartBeatDuration = duration
	r.heartBeat = beat
}

func (r *LazyReceiver) Digest() {
	consumer := r.engine.GetRedis().NewStreamGroupConsumer("default-consumer", "orm-log-group",
		true, 100, r.block, lazyChannelName)
	if r.disableLoop {
		consumer.DisableLoop()
	}
	if r.heartBeat != nil {
		consumer.SetHeartBeat(r.heartBeatDuration, r.heartBeat)
	}
	consumer.Consume(func(streams []redis.XStream, ack *RedisStreamGroupAck) {
		for _, item := range streams[0].Messages {
			var data map[string]interface{}
			_ = jsoniter.ConfigFastest.Unmarshal([]byte(item.Values["v"].(string)), &data)
			ids := r.handleQueries(r.engine, data)
			r.handleClearCache(data, "cl", ids)
			r.handleClearCache(data, "cr", ids)
			ack.Ack(lazyChannelName, item)
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
					engine.Log().Error(err, nil)
					engine.DataDog().RegisterAPMError(err)
				}
			}()
			res := db.Exec(sql, attributes...)
			if sql[0:11] == "INSERT INTO" {
				ids[i] = res.LastInsertId()
			} else {
				ids[i] = 0
			}
		}()
	}
	return ids
}

func (r *LazyReceiver) handleClearCache(validMap map[string]interface{}, key string, ids []uint64) {
	keys, has := validMap[key]
	if has {
		idKey := 0
		validKeys := keys.(map[string]interface{})
		for cacheCode, allKeys := range validKeys {
			validAllKeys := allKeys.([]interface{})
			stringKeys := make([]string, len(validAllKeys))
			for i, v := range validAllKeys {
				parts := strings.Split(v.(string), ":")
				l := len(parts)
				if l == 3 {
					if parts[l-1] == "0" {
						parts[l-1] = strconv.FormatUint(ids[idKey], 10)
					}
					idKey++
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
