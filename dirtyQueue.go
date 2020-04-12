package orm

import (
	"fmt"
)

type DirtyQueueValue struct {
	EntityName string
	ID         uint64
	Added      bool
	Updated    bool
	Deleted    bool
}

type DirtyQueueSender interface {
	Send(engine *Engine, code string, values []*DirtyQueueValue) error
}

type RedisDirtyQueueSender struct {
	PoolName string
}

func (s *RedisDirtyQueueSender) Send(engine *Engine, code string, values []*DirtyQueueValue) error {
	r, has := engine.GetRedis(s.PoolName)
	if !has {
		return RedisCachePoolNotRegisteredError{Name: s.PoolName}
	}
	members := make([]interface{}, len(values))
	for i, val := range values {
		action := "u"
		if val.Added {
			action = "i"
		} else if val.Deleted {
			action = "d"
		}
		key := fmt.Sprintf("%s:%d", val.EntityName+":"+action, val.ID)
		members[i] = key
	}
	_, err := r.SAdd(code, members...)
	return err
}
