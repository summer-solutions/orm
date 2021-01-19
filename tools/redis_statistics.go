package tools

import (
	"github.com/summer-solutions/orm"
)

type RedisStatistics struct {
	RedisPool string
	Info      string
}

func GetRedisStatistics(engine *orm.Engine) []*RedisStatistics {
	pools := engine.GetRegistry().GetRedisPools()
	results := make([]*RedisStatistics, len(pools))
	for i, pool := range pools {
		poolStats := &RedisStatistics{RedisPool: pool}
		r := engine.GetRedis(pool)
		poolStats.Info = r.Info("everything")
		results[i] = poolStats
	}
	return results
}
