package orm

import (
	"encoding/json"
	"time"

	"github.com/juju/errors"
)

type LogQueueValue struct {
	PoolName  string
	TableName string
	ID        uint64
	Meta      map[string]interface{}
	Data      map[string]interface{}
	Updated   time.Time
}

type LogQueueSender interface {
	Send(engine *Engine, values []*LogQueueValue) error
}

type RedisLogQueueSender struct {
	PoolName string
}

func (s *RedisLogQueueSender) Send(engine *Engine, values []*LogQueueValue) error {
	r, _ := engine.GetRedis(s.PoolName)
	members := make([]interface{}, len(values))
	for i, val := range values {
		val.Meta = engine.logMetaData
		asJSON, err := json.Marshal(val)
		if err != nil {
			return errors.Trace(err)
		}
		members[i] = string(asJSON)
	}
	_, err := r.LPush("_log_queue", members...)
	return err
}
