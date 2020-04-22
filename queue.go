package orm

type QueueSender interface {
	Send(engine *Engine, queueCode string, values []string) error
}

type RedisQueueSender struct {
	PoolName string
}

func (s *RedisQueueSender) Send(engine *Engine, queueCode string, values []string) error {
	r := engine.GetRedis(s.PoolName)
	members := make([]interface{}, len(values))
	for i, val := range values {
		members[i] = val
	}
	_, err := r.LPush(queueCode, members...)
	return err
}
