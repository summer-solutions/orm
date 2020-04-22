package orm

import (
	"time"
)

type LogQueueValue struct {
	PoolName  string
	TableName string
	ID        uint64
	Meta      map[string]interface{}
	Data      map[string]interface{}
	Updated   time.Time
}

type QueueSender interface {
	Send(engine *Engine, values []string) error
}

type RedisLogQueueSender struct {
	PoolName string
}

func (s *RedisLogQueueSender) Send(engine *Engine, values []string) error {
	r := engine.GetRedis(s.PoolName)
	members := make([]interface{}, len(values))
	for i, val := range values {
		members[i] = val
	}
	_, err := r.LPush("_log_queue", members...)
	return err
}
