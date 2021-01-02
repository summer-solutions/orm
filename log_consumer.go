package orm

import (
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"

	jsoniter "github.com/json-iterator/go"
)

const logChannelName = "orm-log-chanel"

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

type LogConsumer struct {
	engine            *Engine
	block             time.Duration
	disableLoop       bool
	Logger            func(log *LogQueueValue)
	heartBeat         func()
	heartBeatDuration time.Duration
}

func NewLogConsumer(engine *Engine) *LogConsumer {
	return &LogConsumer{engine: engine, block: time.Minute}
}

func (r *LogConsumer) SetBlock(duration time.Duration) {
	r.block = duration
}

func (r *LogConsumer) SetHeartBeat(duration time.Duration, beat func()) {
	r.heartBeatDuration = duration
	r.heartBeat = beat
}

func (r *LogConsumer) DisableLoop() {
	r.disableLoop = true
}

func (r *LogConsumer) Digest() {
	consumer := r.engine.GetRedis().NewStreamGroupConsumer("default-consumer", "orm-log-group",
		true, 100, r.block, logChannelName)
	if r.disableLoop {
		consumer.DisableLoop()
	}
	if r.heartBeat != nil {
		consumer.SetHeartBeat(r.heartBeatDuration, r.heartBeat)
	}
	consumer.Consume(func(streams []redis.XStream, ack *RedisStreamGroupAck) {
		for _, item := range streams[0].Messages {
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
				if r.Logger != nil {
					poolDB.Begin()
				}
				defer poolDB.Rollback()
				res := poolDB.Exec(query, value.ID, value.Updated.Format("2006-01-02 15:04:05"), meta, before, changes)
				if r.Logger != nil {
					value.LogID = res.LastInsertId()
					r.Logger(&value)
				}
				poolDB.Commit()
				ack.Ack(logChannelName, item)
			}()
		}
	})
}
