package orm

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
)

const countPending = 100
const pendingClaimCheckDuration = time.Minute * 5

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

func (r *RedisCache) NewStreamGroupConsumer(name, group string, autoDelete bool, count, maxScripts int, streams ...string) RedisStreamGroupConsumer {
	return &redisStreamGroupConsumer{redis: r, name: name, streams: streams, group: group, maxScripts: maxScripts, autoDelete: autoDelete,
		loop: true, count: count, block: time.Minute, lockTTL: time.Minute + time.Second*20, lockTick: time.Minute, minIdle: time.Minute * 2}
}

type redisStreamGroupConsumer struct {
	redis             *RedisCache
	name              string
	nr                int
	streams           []string
	group             string
	autoDelete        bool
	loop              bool
	maxScripts        int
	count             int
	block             time.Duration
	heartBeatTime     time.Time
	heartBeat         func()
	heartBeatDuration time.Duration
	lockTTL           time.Duration
	lockTick          time.Duration
	minIdle           time.Duration
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
	nr := 0
	lockName := uniqueLockKey
	var lock *Lock
	for {
		nr++
		if r.maxScripts > 1 {
			lockName = fmt.Sprintf("%s-%d", uniqueLockKey, nr)
		}
		locked, has := locker.Obtain(ctx, lockName, r.lockTTL, time.Millisecond*10)
		if !has {
			if r.maxScripts > 1 && nr < r.maxScripts {
				continue
			}
			panic(fmt.Errorf("consumer %s for group %s is running already", r.getName(), r.group))
		}
		lock = locked
		if r.maxScripts > 1 {
			r.nr = nr
		}
		break
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
	keys := make([]string, 0)
	if r.maxScripts > 1 {
		keys = append(keys, "pending")
	}
	keys = append(keys, "0", ">")
	streams := make([]string, len(r.streams)*2)
	hasInvalid := true
	if r.heartBeat != nil {
		r.heartBeatTime = time.Now()
	}
	pendingChecked := false
	var pendingCheckedTime time.Time
	for {
	KEYS:
		for _, key := range keys {
			invalidCheck := key == "0"
			pendingCheck := key == "pending"
			if pendingCheck {
				if pendingChecked && time.Since(pendingCheckedTime) < pendingClaimCheckDuration {
					continue
				}
				for _, stream := range r.streams {
					start := "-"
					for {
						end := fmt.Sprintf("%d", time.Now().Add(-r.minIdle).UnixNano()/1000000)
						pending := r.redis.XPendingExt(&redis.XPendingExtArgs{Stream: stream, Group: r.group, Start: start, End: end, Count: countPending})
						if len(pending) == 0 {
							break
						}
						ids := make([]string, 0)
						for _, row := range pending {
							if row.Consumer != r.getName() && row.Idle >= r.minIdle {
								ids = append(ids, row.ID)
							}
							start = r.incrementLastID(row.ID)
						}
						if len(ids) > 0 {
							arg := &redis.XClaimArgs{Consumer: r.getName(), Stream: stream, Group: r.group, MinIdle: r.minIdle, Messages: ids}
							r.redis.XClaimJustID(arg)
						}
						if len(pending) < countPending {
							break
						}
					}
				}
				pendingChecked = true
				pendingCheckedTime = time.Now()
				continue
			}
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
					panic(fmt.Errorf("consumer %s for group %s lost lock", r.getName(), r.group))
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
				a := &redis.XReadGroupArgs{Consumer: r.getName(), Group: r.group, Streams: streams, Count: int64(r.count), Block: r.block}
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
							lastIDs[row.Stream] = r.incrementLastID(row.Messages[l-1].ID)
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

func (r *redisStreamGroupConsumer) getName() string {
	if r.maxScripts > 1 {
		return fmt.Sprintf("%s-%d", r.name, r.nr)
	}
	return r.name
}

func (r *redisStreamGroupConsumer) incrementLastID(id string) string {
	s := strings.Split(id, "-")
	counter, _ := strconv.Atoi(s[1])
	return fmt.Sprintf("%s-%d", s[0], counter+1)
}
