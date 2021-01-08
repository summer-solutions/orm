package tools

import (
	"strconv"
	"strings"
	"time"

	"github.com/summer-solutions/orm"
)

type RedisChannelStatistics struct {
	Stream    string
	RedisPool string
	Len       uint64
	MaxLen    uint64
	Groups    []*RedisChannelGroupStatistics
}

type RedisChannelGroupStatistics struct {
	Group                 string
	Pending               uint64
	LastDeliveredID       string
	LastDeliveredDuration time.Duration
	Lower                 string
	LowerDuration         time.Duration
	Higher                string
	HigherDuration        time.Duration
	Consumers             []*RedisChannelConsumerStatistics
}

type RedisChannelConsumerStatistics struct {
	Name    string
	Pending uint64
}

func GetRedisChannelsStatistics(engine *orm.Engine) []*RedisChannelStatistics {
	now := time.Now()
	results := make([]*RedisChannelStatistics, 0)
	for redisPool, channels := range engine.GetRegistry().GetRedisChannels() {
		for stream, max := range channels {
			stat := &RedisChannelStatistics{Stream: stream, MaxLen: max, RedisPool: redisPool}
			results = append(results, stat)
			stat.Groups = make([]*RedisChannelGroupStatistics, 0)
			r := engine.GetRedis(redisPool)
			stat.Len = uint64(r.XLen(stream))
			for _, group := range r.XInfoGroups(stream) {
				groupStats := &RedisChannelGroupStatistics{Group: group.Name, Pending: uint64(group.Pending)}
				groupStats.LastDeliveredID = group.LastDeliveredID
				groupStats.LastDeliveredDuration = idToSince(group.LastDeliveredID, now)
				groupStats.Consumers = make([]*RedisChannelConsumerStatistics, 0)

				pending := r.XPending(stream, group.Name)
				if pending.Count > 0 {
					groupStats.Lower = pending.Lower
					groupStats.LowerDuration = idToSince(pending.Lower, now)
					groupStats.Higher = pending.Higher
					groupStats.HigherDuration = idToSince(pending.Higher, now)

					for name, pending := range pending.Consumers {
						consumer := &RedisChannelConsumerStatistics{Name: name, Pending: uint64(pending)}
						groupStats.Consumers = append(groupStats.Consumers, consumer)
					}
				}
				stat.Groups = append(stat.Groups, groupStats)
			}
		}
	}
	return results
}

func idToSince(id string, now time.Time) time.Duration {
	if id == "" || id == "0-0" {
		return time.Duration(0)
	}
	unixInt, _ := strconv.ParseInt(strings.Split(id, "-")[0], 10, 64)
	unix := time.Unix(0, unixInt*1000000)
	s := now.Sub(unix)
	if s < 0 {
		return time.Duration(0)
	}
	return s
}
