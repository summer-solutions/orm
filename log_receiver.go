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
	err = jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal([]byte(asJSON), &value)
	if err != nil {
		return false, errors.Trace(err)
	}

	poolDB := r.engine.GetMysql(value.PoolName)
	/* #nosec */
	query := fmt.Sprintf("INSERT INTO `%s`(`entity_id`, `added_at`, `meta`, `data`) VALUES(?, ?, ?, ?)", value.TableName)
	var meta, data interface{}
	if value.Meta != nil {
		meta, _ = jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(value.Meta)
	}
	if value.Data != nil {
		data, _ = jsoniter.ConfigCompatibleWithStandardLibrary.Marshal(value.Data)
	}
	_, err = poolDB.Exec(query, value.ID, value.Updated.Format("2006-01-02 15:04:05"), meta, data)
	if err != nil {
		return false, errors.Annotatef(err, "error during log insert query %s", err.Error())
	}
	return true, r.queueSenderReceiver.Flush(r.engine, logQueueName)
}
