package orm

import (
	"fmt"

	jsoniter "github.com/json-iterator/go"

	"github.com/juju/errors"
)

const logQueueName = "_log_queue"

type LogReceiver struct {
	engine    *Engine
	queueName string
	Logger    func(log *LogQueueValue) error
}

func NewLogReceiver(engine *Engine) *LogReceiver {
	return &LogReceiver{engine: engine, queueName: logQueueName}
}

func (r *LogReceiver) QueueName() string {
	return r.queueName
}

func (r *LogReceiver) Digest(item []byte) error {
	var value LogQueueValue
	_ = jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(item, &value)

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
	return nil
}
