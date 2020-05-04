package orm

import (
	"fmt"

	jsoniter "github.com/json-iterator/go"

	"github.com/juju/errors"
)

const logQueueName = "_log_queue"

type LogReceiver struct {
	engine              *Engine
	queueSenderReceiver QueueSenderReceiver
	Logger              func(log *LogQueueValue) error
}

func NewLogReceiver(engine *Engine, queueSenderReceiver QueueSenderReceiver) *LogReceiver {
	return &LogReceiver{engine: engine, queueSenderReceiver: queueSenderReceiver}
}

func (r *LogReceiver) Size() (int64, error) {
	return r.queueSenderReceiver.Size(r.engine, logQueueName)
}

func (r *LogReceiver) Digest() (has bool, err error) {
	has, asJSON, err := r.queueSenderReceiver.Receive(r.engine, logQueueName)
	if err != nil {
		return false, errors.Trace(err)
	}
	if !has {
		return false, nil
	}
	var value LogQueueValue
	_ = jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal([]byte(asJSON), &value)

	poolDB := r.engine.GetMysql(value.PoolName)
	/* #nosec */
	query := fmt.Sprintf("INSERT INTO `%s`(`entity_id`, `added_at`, `meta`, `before`, `changes`) VALUES(?, ?, ?, ?, ?)", value.TableName)
	var meta, before, changes interface{}
	if value.Meta != nil {
		meta, _ = jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(value.Meta)
	}
	if value.Before != nil {
		before, _ = jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(value.Before)
	}
	if value.Changes != nil {
		changes, _ = jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(value.Changes)
	}
	if r.Logger != nil {
		err = r.Logger(&value)
		if err != nil {
			return true, errors.Trace(err)
		}
	}
	_, err = poolDB.Exec(query, value.ID, value.Updated.Format("2006-01-02 15:04:05"), meta, before, changes)
	if err != nil {
		return false, errors.Trace(err)
	}
	return true, r.queueSenderReceiver.Flush(r.engine, logQueueName)
}
