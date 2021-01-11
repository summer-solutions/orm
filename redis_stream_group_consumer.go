package orm

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisStreamGroupHandler func(streams []redis.XStream, ack *RedisStreamGroupAck)

type RedisStreamGroupAck struct {
	consumer *redisStreamGroupConsumer
	ids      map[string][]string
	max      map[string]int
}

func (s *RedisStreamGroupAck) Ack(stream string, message ...redis.XMessage) {
	start := s.max[stream]
	i := start
	for _, m := range message {
		s.ids[stream][i] = m.ID
		i++
	}
	s.max[stream] = i
	if s.consumer.autoDelete {
		// todo in pipeline and transaction
		s.consumer.redis.XAck(stream, s.consumer.group, s.ids[stream][start:i]...)
		s.consumer.redis.XDel(stream, s.ids[stream][start:i]...)
	} else {
		s.consumer.redis.XAck(stream, s.consumer.group, s.ids[stream][start:i]...)
	}
}

type RedisStreamGroupConsumer interface {
	Consume(ctx context.Context, handler RedisStreamGroupHandler)
	DisableLoop()
	SetHeartBeat(duration time.Duration, beat func())
}

func (r *RedisCache) NewStreamGroupConsumer(name, group string, autoDelete bool, count int, streams ...string) RedisStreamGroupConsumer {
	return r.newStreamGroupConsumer(name, group, false, autoDelete, count, streams...)
}

func (r *RedisCache) NewStreamGroupAutoScaledConsumer(prefix, group string, autoDelete bool, count int, streams ...string) RedisStreamGroupConsumer {
	return r.newStreamGroupConsumer(prefix, group, true, autoDelete, count, streams...)
}

func (r *RedisCache) newStreamGroupConsumer(name, group string, autoScale, autoDelete bool, count int, streams ...string) RedisStreamGroupConsumer {
	return &redisStreamGroupConsumer{redis: r, name: name, streams: streams, group: group, autoScale: autoScale, autoDelete: autoDelete,
		loop: true, count: count, block: time.Minute, lockTTL: time.Minute + time.Second*20, lockTick: time.Minute}
}

type redisStreamGroupConsumer struct {
	redis             *RedisCache
	name              string
	nr                string
	streams           []string
	group             string
	autoDelete        bool
	loop              bool
	autoScale         bool
	count             int
	block             time.Duration
	heartBeatTime     time.Time
	heartBeat         func()
	heartBeatDuration time.Duration
	lockTTL           time.Duration
	lockTick          time.Duration
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

func (r *redisStreamGroupConsumer) Consume(ctx context.Context, handler RedisStreamGroupHandler) {
	uniqueLockKey := r.group + "_" + r.name
	locker := r.redis.engine.GetLocker()
	if r.autoScale {
		freeNumber := 0
		func() {
			lockScripts, has := locker.Obtain(ctx, uniqueLockKey+"_lock", time.Second*2, time.Second)
			if !has {
				panic(fmt.Errorf("unable to aquire lock"))
			}
			defer lockScripts.Release()
			runningScripts := r.redis.HGetAll(uniqueLockKey + "_running")
			max := 1
			for k := range runningScripts {
				nr, _ := strconv.Atoi(k)
				if max == 0 || nr > max {
					max = nr
				}
			}
			for freeNumber = 1; freeNumber <= max+1; freeNumber++ {
				timestampString, has := runningScripts[strconv.Itoa(freeNumber)]
				if !has {
					break
				}
				timestamp, _ := strconv.ParseInt(timestampString, 10, 64)
				if time.Since(time.Unix(0, timestamp)) > r.lockTTL {
					break
				}
			}
			r.nr = strconv.Itoa(freeNumber)
			r.redis.HSet(uniqueLockKey+"_running", r.nr, time.Now().UnixNano())
		}()
		defer func() {
			r.redis.HDel(uniqueLockKey+"_running", r.nr)
		}()
	}
	lock, has := locker.Obtain(ctx, uniqueLockKey, r.lockTTL, time.Millisecond)
	if !has {
		panic(fmt.Errorf("consumer %s for group %s is running already", r.name, r.group))
	}
	defer lock.Release()
	ticker := time.NewTicker(r.lockTick)
	done := make(chan bool, 2)
	hasLock := true
	canceled := false
	lockAcquired := time.Now()
	go func() {
		for {
			select {
			case <-ctx.Done():
				canceled = true
				return
			case <-done:
				return
			case <-ticker.C:
				if !lock.Refresh(ctx, r.lockTTL) {
					hasLock = false
					return
				}
				lockAcquired = time.Now()
			}
		}
	}()
	defer func() {
		ticker.Stop()
		done <- true
	}()
	ack := &RedisStreamGroupAck{max: make(map[string]int), ids: make(map[string][]string), consumer: r}
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
				if canceled {
					return
				}
				if !hasLock || time.Since(lockAcquired) > r.lockTTL {
					panic(fmt.Errorf("consumer %s for group %s lost lock", r.name, r.group))
				}
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
				if canceled {
					return
				}
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
					totalACK += ack.max[stream]
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
