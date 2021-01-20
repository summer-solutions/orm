package tools

import (
	"strings"

	"github.com/summer-solutions/orm"
)

type RedisStatistics struct {
	RedisPool string
	Info      map[string]string
}

func GetRedisStatistics(engine *orm.Engine) []*RedisStatistics {
	pools := engine.GetRegistry().GetRedisPools()
	results := make([]*RedisStatistics, len(pools))
	for i, pool := range pools {
		poolStats := &RedisStatistics{RedisPool: pool, Info: make(map[string]string)}
		r := engine.GetRedis(pool)
		info := r.Info()
		lines := strings.Split(info, "\r\n")
		for _, line := range lines {
			if line == "" || strings.HasPrefix(line, "#") {
				continue
			}
			row := strings.Split(line, ":")
			val := ""
			if len(row) > 1 {
				val = row[1]
			}
			poolStats.Info[row[0]] = val
		}
		results[i] = poolStats
	}
	return results
}
