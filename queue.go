package orm

type QueueSender interface {
	Send(engine *Engine, queueCode string, values [][]byte) error
}

type RedisQueueSender struct {
	PoolName string
}

func (r *RedisQueueSender) Send(engine *Engine, queueCode string, values [][]byte) error {
	members := make([]interface{}, len(values))
	for i, val := range values {
		members[i] = val
	}
	_, err := engine.GetRedis(r.PoolName).LPush(queueCode, members...)
	return err
}
