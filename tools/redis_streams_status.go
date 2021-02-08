package tools

import (
	"strconv"
	"strings"
	"time"

	"github.com/summer-solutions/orm"
)

type RedisStreamStatistics struct {
	Stream    string
	RedisPool string
	Len       uint64
	Groups    []*RedisStreamGroupStatistics
}

type RedisStreamGroupStatistics struct {
	Group                 string
	Pending               uint64
	LastDeliveredID       string
	LastDeliveredDuration string
	Lower                 string
	LowerDuration         string
	Higher                string
	HigherDuration        string
	Consumers             []*RedisStreamConsumerStatistics
}

type RedisStreamConsumerStatistics struct {
	Name                  string
	Pending               uint64
	SpeedEventNanoseconds float32
	SpeedLastMeasurement  *time.Time
}

func GetRedisStreamsStatistics(engine *orm.Engine) []*RedisStreamStatistics {
	now := time.Now()
	results := make([]*RedisStreamStatistics, 0)
	for redisPool, channels := range engine.GetRegistry().GetRedisStreams() {
		r := engine.GetRedis(redisPool)
		speedStats := r.HGetAll("_orm_ss")
		for stream := range channels {
			stat := &RedisStreamStatistics{Stream: stream, RedisPool: redisPool}
			results = append(results, stat)
			stat.Groups = make([]*RedisStreamGroupStatistics, 0)
			stat.Len = uint64(r.XLen(stream))
			for _, group := range r.XInfoGroups(stream) {
				groupStats := &RedisStreamGroupStatistics{Group: group.Name, Pending: uint64(group.Pending)}
				groupStats.LastDeliveredID = group.LastDeliveredID
				groupStats.LastDeliveredDuration = idToSince(group.LastDeliveredID, now)
				groupStats.Consumers = make([]*RedisStreamConsumerStatistics, 0)

				pending := r.XPending(stream, group.Name)
				if pending.Count > 0 {
					groupStats.Lower = pending.Lower
					groupStats.LowerDuration = idToSince(pending.Lower, now)
					groupStats.Higher = pending.Higher
					groupStats.HigherDuration = idToSince(pending.Higher, now)

					for name, pending := range pending.Consumers {
						speed := float32(0)
						var speedLastTime *time.Time
						speedKey := group.Name + "_" + redisPool + "_" + name
						speedEvents, has := speedStats[speedKey+"e"]
						if has {
							speedEventsAsInt, _ := strconv.Atoi(speedEvents)
							if speedEventsAsInt > 0 {
								speedTime, _ := speedStats[speedKey+"t"]
								speedTimeAsInt, _ := strconv.Atoi(speedTime)
								speed = float32(speedTimeAsInt / speedEventsAsInt)
								speedUnix, _ := speedStats[speedKey+"u"]
								speedUnixAsInt, _ := strconv.Atoi(speedUnix)
								t := time.Unix(int64(speedUnixAsInt), 0)
								speedLastTime = &t
							}
						}
						consumer := &RedisStreamConsumerStatistics{Name: name, Pending: uint64(pending), SpeedEventNanoseconds: speed,
							SpeedLastMeasurement: speedLastTime}
						groupStats.Consumers = append(groupStats.Consumers, consumer)
					}
				}
				stat.Groups = append(stat.Groups, groupStats)
			}
		}
	}
	return results
}

func idToSince(id string, now time.Time) string {
	if id == "" || id == "0-0" {
		return "0"
	}
	unixInt, _ := strconv.ParseInt(strings.Split(id, "-")[0], 10, 64)
	unix := time.Unix(0, unixInt*1000000)
	s := now.Sub(unix)
	if s < 0 {
		return "0"
	}
	return s.String()
}
