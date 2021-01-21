package orm

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
)

const countPending = 100
const garbageCollectorCount = 100
const maxConsumers = 100
const pendingClaimCheckDuration = time.Minute * 2

type EventAsMap map[string]interface{}

type Event interface {
	Ack()
	Skip()
	ID() string
	Stream() string
	RawData() map[string]interface{}
	Unserialize(val interface{}) error
}

type event struct {
	consumer *eventsConsumer
	stream   string
	message  redis.XMessage
	ack      bool
	skip     bool
}

func (ev *event) Ack() {
	ev.consumer.redis.XAck(ev.stream, ev.consumer.group, ev.message.ID)
	ev.ack = true
}

func (ev *event) Skip() {
	ev.skip = true
}

func (ev *event) ID() string {
	return ev.message.ID
}

func (ev *event) Stream() string {
	return ev.stream
}

func (ev *event) RawData() map[string]interface{} {
	return ev.message.Values
}

func (ev *event) Unserialize(value interface{}) error {
	val, has := ev.message.Values["_s"]
	if !has {
		return fmt.Errorf("event without struct data")
	}
	return jsoniter.ConfigFastest.Unmarshal([]byte(val.(string)), &value)
}

type EventBroker interface {
	PublishMap(stream string, event EventAsMap) (id string)
	Publish(stream string, event interface{}) (id string)
	Consumer(name, group string) EventsConsumer
}

type eventBroker struct {
	engine *Engine
}

func (e *Engine) GetEventBroker() EventBroker {
	if e.eventBroker == nil {
		e.eventBroker = &eventBroker{engine: e}
	}
	return e.eventBroker
}

func (eb *eventBroker) PublishMap(stream string, event EventAsMap) (id string) {
	pool, has := eb.engine.registry.redisStreamPools[stream]
	if !has {
		panic(fmt.Errorf("unregistered stream %s", stream))
	}
	var v map[string]interface{} = event
	id = eb.engine.GetRedis(pool).xAdd(stream, v)
	return id
}

func (eb *eventBroker) Publish(stream string, event interface{}) (id string) {
	asJSON, err := jsoniter.ConfigFastest.Marshal(event)
	if err != nil {
		panic(err)
	}
	return eb.PublishMap(stream, EventAsMap{"_s": string(asJSON)})
}

type EventConsumerHandler func([]Event)

type EventsConsumer interface {
	Consume(ctx context.Context, count int, blocking bool, handler EventConsumerHandler)
	DisableLoop()
	SetHeartBeat(duration time.Duration, beat func())
}

func (eb *eventBroker) Consumer(name, group string) EventsConsumer {
	streams := make([]string, 0)
	for _, row := range eb.engine.registry.redisStreamGroups {
		for stream, groups := range row {
			_, has := groups[group]
			if has {
				streams = append(streams, stream)
			}
		}
	}
	if len(streams) == 0 {
		panic(fmt.Errorf("unregistered streams for group %s", group))
	}
	redisPool := ""
	for _, stream := range streams {
		pool := eb.engine.registry.redisStreamPools[stream]
		if redisPool == "" {
			redisPool = pool
		} else if redisPool != pool {
			panic(fmt.Errorf("reading from different redis pool not allowed"))
		}
	}
	return &eventsConsumer{redis: eb.engine.GetRedis(redisPool), name: name, streams: streams, group: group,
		loop: true, block: time.Second * 30, lockTTL: time.Minute, lockTick: time.Second * 50,
		garbageTick: time.Second * 30, garbageLock: time.Minute, minIdle: pendingClaimCheckDuration, claimDuration: pendingClaimCheckDuration}
}

type eventsConsumer struct {
	redis             *RedisCache
	name              string
	nr                int
	deadConsumers     int
	nrString          string
	streams           []string
	group             string
	loop              bool
	block             time.Duration
	heartBeatTime     time.Time
	heartBeat         func()
	heartBeatDuration time.Duration
	lockTTL           time.Duration
	lockTick          time.Duration
	garbageTick       time.Duration
	garbageLock       time.Duration
	minIdle           time.Duration
	claimDuration     time.Duration
}

func (r *eventsConsumer) DisableLoop() {
	r.loop = false
}

func (r *eventsConsumer) SetHeartBeat(duration time.Duration, beat func()) {
	r.heartBeat = beat
	r.heartBeatDuration = duration
}

func (r *eventsConsumer) HeartBeat(force bool) {
	if r.heartBeat != nil && (force || time.Since(r.heartBeatTime) >= r.heartBeatDuration) {
		r.heartBeat()
		r.heartBeatTime = time.Now()
	}
}

func (r *eventsConsumer) Consume(ctx context.Context, count int, blocking bool, handler EventConsumerHandler) {
	uniqueLockKey := r.group + "_" + r.name + "_" + r.redis.code
	runningKey := uniqueLockKey + "_running"
	locker := r.redis.engine.GetLocker()
	nr := 0
	var lockName string
	var lock *Lock
	for {
		nr++
		lockName = fmt.Sprintf("%s-%d", uniqueLockKey, nr)
		locked, has := locker.Obtain(ctx, lockName, r.lockTTL, 0)
		if !has {
			if nr < maxConsumers {
				continue
			}
			panic(fmt.Errorf("consumer %s for group %s limit %d reached", r.getName(), r.group, maxConsumers))
		}
		lock = locked
		r.nr = nr
		r.nrString = strconv.Itoa(nr)
		r.redis.HSet(runningKey, r.nrString, fmt.Sprintf("%d", time.Now().Unix()))
		break
	}
	ticker := time.NewTicker(r.lockTick)
	done := make(chan bool)
	defer func() {
		lock.Release()
		ticker.Stop()
		close(done)
	}()
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
				now := time.Now()
				lockAcquired = now
				r.redis.HSet(runningKey, r.nrString, fmt.Sprintf("%d", now.Unix()))
			}
		}
	}()
	garbageTicker := time.NewTicker(r.garbageTick)
	go func() {
		r.garbageCollector(ctx)
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-garbageTicker.C:
				r.garbageCollector(ctx)
			}
		}
	}()

	lastIDs := make(map[string]string)
	for _, stream := range r.streams {
		r.redis.XGroupCreateMkStream(stream, r.group, "0")
	}
	keys := []string{"pending", "0", ">"}
	streams := make([]string, len(r.streams)*2)
	hasInvalid := true
	if r.heartBeat != nil {
		r.heartBeatTime = time.Now()
	}
	pendingChecked := false
	var pendingCheckedTime time.Time
	b := r.block
	if !blocking {
		b = -1
	}
	for {
	KEYS:
		for _, key := range keys {
			invalidCheck := key == "0"
			pendingCheck := key == "pending"
			normalCheck := key == ">"
			started := time.Now()
			if pendingCheck {
				if pendingChecked && time.Since(pendingCheckedTime) < r.claimDuration {
					continue
				}

				r.deadConsumers = 0
				all := r.redis.HGetAll(runningKey)
				for k, v := range all {
					if k == r.nrString {
						continue
					}
					unix, _ := strconv.ParseInt(v, 10, 64)
					if time.Since(time.Unix(unix, 0)) >= r.claimDuration {
						r.deadConsumers++
					}
				}
				if r.deadConsumers == 0 {
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
							start = r.incrementID(row.ID)
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
				a := &redis.XReadGroupArgs{Consumer: r.getName(), Group: r.group, Streams: streams, Count: int64(count), Block: b}
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
					} else if !blocking && normalCheck {
						time.Sleep(time.Second * 10)
					}
					continue KEYS
				}
				events := make([]Event, totalMessages)
				i = 0
				for _, row := range results {
					for _, message := range row.Messages {
						events[i] = &event{stream: row.Stream, message: message, consumer: r}
						i++
					}
				}
				handler(events)
				totalACK := 0
				var toAck map[string][]string
				for _, ev := range events {
					ev := ev.(*event)
					if ev.ack {
						totalACK++
					} else if !ev.skip {
						if toAck == nil {
							toAck = make(map[string][]string)
						} else if toAck[ev.stream] == nil {
							toAck[ev.stream] = make([]string, 0)
						}
						toAck[ev.stream] = append(toAck[ev.stream], ev.message.ID)
						totalACK++
					}
				}
				for stream, ids := range toAck {
					r.redis.XAck(stream, r.group, ids...)
				}
				if totalACK < totalMessages {
					hasInvalid = true
				}
				if r.deadConsumers > 0 && time.Since(pendingCheckedTime) >= r.claimDuration {
					break
				}
				if normalCheck && hasInvalid && totalMessages < count && time.Since(started) > time.Minute*2 {
					break
				}
			}
		}
		if !r.loop {
			r.HeartBeat(true)
			break
		}
	}
}

func (r *eventsConsumer) getName() string {
	return fmt.Sprintf("%s-%d", r.name, r.nr)
}

func (r *eventsConsumer) incrementID(id string) string {
	s := strings.Split(id, "-")
	counter, _ := strconv.Atoi(s[1])
	return fmt.Sprintf("%s-%d", s[0], counter+1)
}

func (r *eventsConsumer) garbageCollector(ctx context.Context) {
	func() {
		defer func() {
			if rec := recover(); rec != nil {
				r.redis.engine.reportError(rec)
			}
		}()
		locker := r.redis.engine.GetLocker()
		def := r.redis.engine.registry.redisStreamGroups[r.redis.code]
		for _, stream := range r.streams {
			_, has := locker.Obtain(ctx, "garbage_"+stream+"_"+r.redis.code, r.garbageLock, 0)
			if !has {
				continue
			}
			info := r.redis.XInfoGroups(stream)
			ids := make(map[string][]int64)
			for name := range def[stream] {
				ids[name] = []int64{0, 0}
			}
			inPending := false
			for _, group := range info {
				_, has := ids[group.Name]
				if !has {
					// TODO remove only if has old events
					//r.redis.XGroupDestroy(stream, group.Name)
					r.redis.engine.log.Warn(fmt.Sprintf("not registered stream group %s in stream %s", group.Name, stream), nil)
					continue
				}
				if group.LastDeliveredID == "" {
					continue
				}
				lastDelivered := group.LastDeliveredID
				pending := r.redis.XPending(stream, group.Name)
				if pending.Lower != "" {
					lastDelivered = pending.Lower
					inPending = true
				}
				s := strings.Split(lastDelivered, "-")
				id, _ := strconv.ParseInt(s[0], 10, 64)
				ids[group.Name][0] = id
				counter, _ := strconv.ParseInt(s[1], 10, 64)
				ids[group.Name][1] = counter
			}
			minID := []int64{-1, 0}
			for _, id := range ids {
				if id[0] == 0 {
					minID[0] = 0
					minID[1] = 0
				} else if minID[0] == -1 || id[0] < minID[0] || (id[0] == minID[0] && id[1] < minID[1]) {
					minID[0] = id[0]
					minID[1] = id[1]
				}
			}
			if minID[0] == 0 {
				continue
			}
			// TODO check of redis 6.2 and use trim with minid
			start := "-"
			var end string
			if inPending {
				if minID[1] > 0 {
					end = fmt.Sprintf("%d-%d", minID[0], minID[1]-1)
				} else {
					end = fmt.Sprintf("%d", minID[0]-1)
				}
			} else {
				end = fmt.Sprintf("%d-%d", minID[0], minID[1])
			}
			for {
				messages := r.redis.XRange(stream, start, end, garbageCollectorCount)
				l := len(messages)
				if l > 0 {
					keys := make([]string, l)
					for i, message := range messages {
						keys[i] = message.ID
					}
					r.redis.XDel(stream, keys...)
					start = r.incrementID(keys[l-1])
				}
				if l < garbageCollectorCount {
					break
				}
			}
		}
	}()
}
