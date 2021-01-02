package orm

import (
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisStreamGroupHandler func(streams []redis.XStream, ack *RedisStreamGroupAck)

type RedisStreamGroupAck struct {
	ids map[string][]string
	max map[string]int
}

func (s *RedisStreamGroupAck) Ack(stream string, message ...redis.XMessage) {
	i := s.max[stream]
	for _, m := range message {
		s.ids[stream][i] = m.ID
		i++
	}
	s.max[stream] = i
}

type RedisStreamGroupConsumer interface {
	Consume(handler RedisStreamGroupHandler)
	DisableLoop()
	SetHeartBeat(duration time.Duration, beat func())
}

func (r *RedisCache) NewStreamGroupConsumer(name, group string, autoDelete bool, count int,
	block time.Duration, streams ...string) RedisStreamGroupConsumer {
	return &redisStreamGroupConsumer{redis: r, name: name, streams: streams, group: group, autoDelete: autoDelete,
		loop: true, count: count, block: block}
}

type redisStreamGroupConsumer struct {
	redis             *RedisCache
	name              string
	streams           []string
	group             string
	autoDelete        bool
	loop              bool
	count             int
	block             time.Duration
	heartBeatTime     time.Time
	heartBeat         func()
	heartBeatDuration time.Duration
}

func (r *redisStreamGroupConsumer) DisableLoop() {
	r.loop = false
}

func (r *redisStreamGroupConsumer) SetHeartBeat(duration time.Duration, beat func()) {
	r.heartBeat = beat
	r.heartBeatDuration = duration
}

func (r *redisStreamGroupConsumer) HeartBeat(force bool) {
	if r.heartBeat != nil && (force || time.Since(r.heartBeatTime) >= r.heartBeatDuration) {
		r.heartBeat()
		r.heartBeatTime = time.Now()
	}
}

func (r *redisStreamGroupConsumer) Consume(handler RedisStreamGroupHandler) {
	ack := &RedisStreamGroupAck{max: make(map[string]int), ids: make(map[string][]string)}
	lastIDs := make(map[string]string)
	for _, stream := range r.streams {
		r.redis.XGroupCreateMkStream(stream, r.group, "0")
		ack.max[stream] = 0
		ack.ids[stream] = make([]string, r.count)
	}
	keys := []string{"0", ">"}
	streams := make([]string, len(r.streams)*2)
	hasInvalid := true
	if r.heartBeat != nil {
		r.heartBeatTime = time.Now()
	}
	for {
	KEYS:
		for _, key := range keys {
			invalidCheck := key == "0"
			if invalidCheck {
				if !hasInvalid {
					continue
				}
				for _, stream := range r.streams {
					lastIDs[stream] = "0"
				}
			}
			for {
				i := 0
				zeroCount := 0
				for _, stream := range r.streams {
					streams[i] = stream
					i++
				}
				for _, stream := range r.streams {
					if invalidCheck {
						streams[i] = lastIDs[stream]
						if lastIDs[stream] == "0" {
							zeroCount++
						}
					} else {
						streams[i] = ">"
					}
					i++
				}
				a := &redis.XReadGroupArgs{Consumer: r.name, Group: r.group, Streams: streams, Count: int64(r.count), Block: r.block}
				results := r.redis.XReadGroup(a)
				totalMessages := 0
				for _, row := range results {
					l := len(row.Messages)
					if l > 0 {
						totalMessages += l
						if invalidCheck {
							lastIDs[row.Stream] = row.Messages[l-1].ID
						}
					}
				}
				r.HeartBeat(false)
				if totalMessages == 0 {
					if invalidCheck && zeroCount == len(r.streams) {
						hasInvalid = false
					}
					continue KEYS
				}

				totalACK := 0
				handler(results, ack)
				for _, stream := range r.streams {
					max := ack.max[stream]
					if max > 0 {
						if r.autoDelete {
							// todo in pipeline and transaction
							r.redis.XAck(stream, r.group, ack.ids[stream][0:max]...)
							r.redis.XDel(stream, ack.ids[stream][0:max]...)
						} else {
							r.redis.XAck(stream, r.group, ack.ids[stream][0:max]...)
						}
					}
					totalACK += max
					ack.max[stream] = 0
				}
				if totalACK < totalMessages {
					hasInvalid = true
				}
			}
		}
		if !r.loop {
			r.HeartBeat(true)
			break
		}
	}
}