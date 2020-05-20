package orm

import (
	"encoding/json"
	"fmt"

	"github.com/juju/errors"
)

const logQueueName = "orm_log"

type LogReceiver struct {
	engine      *Engine
	disableLoop bool
	Logger      func(log *LogQueueValue) error
}

func NewLogReceiver(engine *Engine) *LogReceiver {
	return &LogReceiver{engine: engine}
}

func (r *LogReceiver) DisableLoop() {
	r.disableLoop = true
}

func (r *LogReceiver) Digest() error {
	channel := r.engine.GetRabbitMQQueue(logQueueName)
	consumer, err := channel.NewConsumer("default consumer")
	if err != nil {
		return errors.Trace(err)
	}
	defer consumer.Close()
	if r.disableLoop {
		consumer.DisableLoop()
	}
	var value LogQueueValue
	err = consumer.Consume(func(items [][]byte) error {
		for _, item := range items {
			_ = json.Unmarshal(item, &value)
			poolDB := r.engine.GetMysql(value.PoolName)
			/* #nosec */
			query := fmt.Sprintf("INSERT INTO `%s`(`entity_id`, `added_at`, `meta`, `before`, `changes`) VALUES(?, ?, ?, ?, ?)", value.TableName)
			var meta, before, changes interface{}
			if value.Meta != nil {
				meta, _ = json.Marshal(value.Meta)
			}
			if value.Before != nil {
				before, _ = json.Marshal(value.Before)
			}
			if value.Changes != nil {
				changes, _ = json.Marshal(value.Changes)
			}
			res, err := poolDB.Exec(query, value.ID, value.Updated.Format("2006-01-02 15:04:05"), meta, before, changes)
			if err != nil {
				return errors.Trace(err)
			}
			if r.Logger != nil {
				id, err := res.LastInsertId()
				if err != nil {
					return errors.Trace(err)
				}
				value.ID = uint64(id)
				return r.Logger(&value)
			}
		}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
