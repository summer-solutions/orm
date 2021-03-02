package tools

import (
	"strconv"
	"strings"

	"github.com/summer-solutions/orm"
)

type RedisSearchStatistics struct {
	Index               *orm.RedisSearchIndex
	Versions            []*RedisSearchStatisticsIndexVersion
	ForceReindex        bool
	ForceReindexVersion uint64
	ForceReindexLastID  uint64
}

type RedisSearchStatisticsIndexVersion struct {
	Info    *orm.RedisSearchIndexInfo
	Current bool
}

func GetRedisSearchStatistics(engine *orm.Engine) []*RedisSearchStatistics {
	result := make([]*RedisSearchStatistics, 0)
	indices := engine.GetRegistry().GetRedisSearchIndices()
	for pool, list := range indices {
		search := engine.GetRedisSearch(pool)
		stamps := engine.GetRedis(pool).HGetAll("_orm_force_index")
		indicesInRedis := search.ListIndices()
		for _, index := range list {
			stat := &RedisSearchStatistics{Index: index, Versions: make([]*RedisSearchStatisticsIndexVersion, 0)}
			current := ""
			info := search.Info(index.Name)
			if info != nil {
				current = info.Name
			}
			for _, inRedis := range indicesInRedis {
				if strings.HasPrefix(inRedis, index.Name+":") {
					info := search.Info(inRedis)
					indexStats := &RedisSearchStatisticsIndexVersion{Info: info, Current: current == inRedis}
					stat.Versions = append(stat.Versions, indexStats)
				}
			}
			stamp, has := stamps[index.Name]
			if has {
				parts := strings.Split(stamp, ":")
				if parts[0] != "ok" {
					stat.ForceReindex = true
					stat.ForceReindexVersion, _ = strconv.ParseUint(parts[1], 10, 64)
					stat.ForceReindexLastID, _ = strconv.ParseUint(parts[0], 10, 64)
				}
			} else {
				stat.ForceReindex = true
			}
			result = append(result, stat)
		}
	}
	return result
}
