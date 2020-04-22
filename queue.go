package orm

import (
	"github.com/juju/errors"
)

type QueueSenderReceiver interface {
	Send(engine *Engine, queueCode string, values []string) error
	Size(engine *Engine, queueCode string) (int64, error)
	Receive(engine *Engine, queueCode string) (has bool, value string, err error)
	Flush(engine *Engine, queueCode string) error
}

type RedisQueueSenderReceiver struct {
	PoolName string
}

func (r *RedisQueueSenderReceiver) Send(engine *Engine, queueCode string, values []string) error {
	members := make([]interface{}, len(values))
	for i, val := range values {
		members[i] = val
	}
	_, err := engine.GetRedis(r.PoolName).LPush(queueCode, members...)
	return err
}

func (r *RedisQueueSenderReceiver) Size(engine *Engine, queueCode string) (int64, error) {
	return engine.GetRedis(r.PoolName).LLen(queueCode)
}

func (r *RedisQueueSenderReceiver) Receive(engine *Engine, queueCode string) (has bool, value string, err error) {
	element, found, err := engine.GetRedis(r.PoolName).RPop(queueCode)
	if err != nil {
		return false, "", errors.Trace(err)
	}
	if !found {
		return false, "", nil
	}
	return true, element, nil
}

func (r *RedisQueueSenderReceiver) Flush(engine *Engine, queueCode string) error {
	return nil
}
