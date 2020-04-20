package orm

import (
	"github.com/juju/errors"
)

type LogReceiverQueue interface {
	Size() (int64, error)
	Receive(max int) ([]*LogQueueValue, error)
	Flush([]*LogQueueValue) error
}

type RedisLogReceiver struct {
	r *RedisCache
}

func (r *RedisLogReceiver) Size() (int64, error) {
	return r.r.LLen("_log_queue")
}

func (r *RedisLogReceiver) Receive(max int) ([]*LogQueueValue, error) {
	//todo
	return nil, nil
}

func (r *RedisLogReceiver) Flush([]*LogQueueValue) error {
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

func (r *LogReceiver) Digest(max int) (has bool, err error) {
	items, err := r.queueReceiver.Receive(max)
	if err != nil {
		return false, errors.Trace(err)
	}
	if len(items) == 0 {
		return false, nil
	}
	//TODO save in DB
	return true, r.queueReceiver.Flush(items)
}
