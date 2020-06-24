package orm

import (
	"fmt"
	"time"

	jsoniter "github.com/json-iterator/go"
)

const logQueueName = "orm_log"

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

type LogReceiver struct {
	engine      *Engine
	disableLoop bool
	Logger      func(log *LogQueueValue)
	heartBeat   func()
}

func NewLogReceiver(engine *Engine) *LogReceiver {
	return &LogReceiver{engine: engine}
}

func (r *LogReceiver) SetHeartBeat(beat func()) {
	r.heartBeat = beat
}

func (r *LogReceiver) DisableLoop() {
	r.disableLoop = true
}

func (r *LogReceiver) Purge() {
	channel := r.engine.GetRabbitMQQueue(logQueueName)
	consumer := channel.NewConsumer("default consumer")
	consumer.Purge()
	consumer.Close()
}

func (r *LogReceiver) Digest() {
	channel := r.engine.GetRabbitMQQueue(logQueueName)
	consumer := channel.NewConsumer("default consumer")
	defer consumer.Close()
	if r.disableLoop {
		consumer.DisableLoop()
	}
	if r.heartBeat != nil {
		consumer.SetHeartBeat(r.heartBeat)
	}
	consumer.Consume(func(items [][]byte) {
		for _, item := range items {
			var value LogQueueValue
			_ = jsoniter.ConfigFastest.Unmarshal(item, &value)
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
			res := poolDB.Exec(query, value.ID, value.Updated.Format("2006-01-02 15:04:05"), meta, before, changes)
			if r.Logger != nil {
				value.LogID = res.LastInsertId()
				r.Logger(&value)
			}
		}
	})
}
