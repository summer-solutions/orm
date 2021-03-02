package tools

import (
	"github.com/summer-solutions/orm"
)

type RedisSearchStatistics struct {
	RedisPool string
}

func GetRedisSearchStatistics(engine *orm.Engine) []*RedisSearchStatistics {
	return nil
}
