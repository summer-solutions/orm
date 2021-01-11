package orm

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"

	jsoniter "github.com/json-iterator/go"
)

const lazyChannelName = "orm-lazy-channel"
const logChannelName = "orm-log-channel"

type LogQueueValue struct {
	PoolName  string
	TableName string
	ID        uint64
	LogID     uint64
	Meta      map[string]interface{}
	Before    map[string]interface{}
	Changes   map[string]interface{}
	Updated   time.Time
}

type AsyncConsumer struct {
	engine            *Engine
	redisPool         string
	block             time.Duration
	disableLoop       bool
	heartBeat         func()
	heartBeatDuration time.Duration
	logLogger         func(log *LogQueueValue)
}

func NewAsyncConsumer(engine *Engine, redisPool string) *AsyncConsumer {
	return &AsyncConsumer{engine: engine, redisPool: redisPool, block: time.Minute}
}

func (r *AsyncConsumer) DisableLoop() {
	r.disableLoop = true
}

func (r *AsyncConsumer) SetHeartBeat(duration time.Duration, beat func()) {
	r.heartBeatDuration = duration
	r.heartBeat = beat
}

func (r *AsyncConsumer) SetLogLogger(logger func(log *LogQueueValue)) {
	r.logLogger = logger
}

func (r *AsyncConsumer) Digest(ctx context.Context) {
	consumer := r.engine.GetRedis(r.redisPool).NewStreamGroupConsumer("default-consumer", "orm-async-group",
		true, 100, lazyChannelName, logChannelName)
	consumer.(*redisStreamGroupConsumer).block = r.block
	if r.disableLoop {
		consumer.DisableLoop()
	}
	if r.heartBeat != nil {
		consumer.SetHeartBeat(r.heartBeatDuration, r.heartBeat)
	}
	consumer.Consume(ctx, func(streams []redis.XStream, ack *RedisStreamGroupAck) {
		for _, stream := range streams {
			if stream.Stream == lazyChannelName {
				r.handleLazy(stream.Messages, ack)
			} else {
				r.handleLog(stream.Messages, ack)
			}
		}
	})
}

func (r *AsyncConsumer) handleLog(messages []redis.XMessage, ack *RedisStreamGroupAck) {
	for _, item := range messages {
		var value LogQueueValue
		_ = jsoniter.ConfigFastest.Unmarshal([]byte(item.Values["v"].(string)), &value)
		poolDB := r.engine.GetMysql(value.PoolName)
		/* #nosec */
		query := fmt.Sprintf("INSERT INTO `%s`(`entity_id`, `added_at`, `meta`, `before`, `changes`) VALUES(?, ?, ?, ?, ?)", value.TableName)
		var meta, before, changes interface{}
		if value.Meta != nil {
			meta, _ = jsoniter.ConfigFastest.Marshal(value.Meta)
		}
		if value.Before != nil {
			before, _ = jsoniter.ConfigFastest.Marshal(value.Before)
		}
		if value.Changes != nil {
			changes, _ = jsoniter.ConfigFastest.Marshal(value.Changes)
		}
		func() {
			if r.logLogger != nil {
				poolDB.Begin()
			}
			defer poolDB.Rollback()
			res := poolDB.Exec(query, value.ID, value.Updated.Format("2006-01-02 15:04:05"), meta, before, changes)
			if r.logLogger != nil {
				value.LogID = res.LastInsertId()
				r.logLogger(&value)
				poolDB.Commit()
			}
			ack.Ack(logChannelName, item)
		}()
	}
}

func (r *AsyncConsumer) handleLazy(messages []redis.XMessage, ack *RedisStreamGroupAck) {
	for _, item := range messages {
		var data map[string]interface{}
		_ = jsoniter.ConfigFastest.Unmarshal([]byte(item.Values["v"].(string)), &data)
		ids := r.handleQueries(r.engine, data)
		r.handleClearCache(data, "cl", ids)
		r.handleClearCache(data, "cr", ids)
		ack.Ack(lazyChannelName, item)
	}
}

func (r *AsyncConsumer) handleQueries(engine *Engine, validMap map[string]interface{}) []uint64 {
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
					if engine.dataDog != nil {
						engine.dataDog.RegisterAPMError(err)
					}
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

func (r *AsyncConsumer) handleClearCache(validMap map[string]interface{}, key string, ids []uint64) {
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
				cache := r.engine.GetLocalCache(cacheCode)
				cache.Remove(stringKeys...)
			} else {
				cache := r.engine.redis[cacheCode]
				cache.Del(stringKeys...)
			}
		}
	}
}
