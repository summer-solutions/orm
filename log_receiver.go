package orm

import (
	"encoding/json"
	"fmt"

	"github.com/juju/errors"
)

type LogReceiverQueue interface {
	Size() (int64, error)
	Receive() (*LogQueueValue, error)
	Flush() error
}

type RedisLogReceiver struct {
	Redis *RedisCache
}

func (r *RedisLogReceiver) Size() (int64, error) {
	return r.Redis.LLen("_log_queue")
}

func (r *RedisLogReceiver) Receive() (*LogQueueValue, error) {
	element, found, err := r.Redis.RPop("_log_queue")
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !found {
		return nil, nil
	}
	var value LogQueueValue
	err = json.Unmarshal([]byte(element), &value)
	if err != nil {
		return nil, errors.Trace(err)
	}
	return &value, nil
}

func (r *RedisLogReceiver) Flush() error {
	return nil
}

type LogReceiver struct {
	engine        *Engine
	queueReceiver LogReceiverQueue
}

func NewLogReceiver(engine *Engine, queueReceiver LogReceiverQueue) *LogReceiver {
	return &LogReceiver{engine: engine, queueReceiver: queueReceiver}
}

func (r *LogReceiver) Size() (int64, error) {
	return r.queueReceiver.Size()
}

func (r *LogReceiver) Digest() (has bool, err error) {
	received, err := r.queueReceiver.Receive()
	if err != nil {
		return false, errors.Trace(err)
	}
	if received == nil {
		return false, nil
	}
	poolDB := r.engine.GetMysql(received.PoolName)
	/* #nosec */
	query := fmt.Sprintf("INSERT INTO `%s`(`entity_id`, `added_at`, `meta`, `data`) VALUES(?, ?, ?, ?)", received.TableName)
	var meta, data interface{}
	if received.Meta != nil {
		meta, _ = json.Marshal(received.Meta)
	}
	if received.Data != nil {
		data, _ = json.Marshal(received.Data)
	}
	_, err = poolDB.Exec(query, received.ID, received.Updated.Format("2006-01-02 15:04:05"), meta, data)
	if err != nil {
		return false, errors.Annotatef(err, "error during log insert query %s", err.Error())
	}
	return true, r.queueReceiver.Flush()
}
