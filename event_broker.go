package orm

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
	jsoniter "github.com/json-iterator/go"
)

const countPending = 100
const maxConsumers = 100
const pendingClaimCheckDuration = time.Minute * 2
const speedHSetKey = "_orm_ss"

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
	NewFlusher() EventFlusher
}

type EventFlusher interface {
	PublishMap(stream string, event EventAsMap)
	Publish(stream string, event interface{})
	Flush()
}

type eventFlusher struct {
	eb     *eventBroker
	mutex  sync.Mutex
	events map[string][]EventAsMap
}

type eventBroker struct {
	engine *Engine
}

func (ef *eventFlusher) PublishMap(stream string, event EventAsMap) {
	ef.mutex.Lock()
	defer ef.mutex.Unlock()
	if ef.events[stream] == nil {
		ef.events[stream] = []EventAsMap{event}
	} else {
		ef.events[stream] = append(ef.events[stream], event)
	}
}

func (ef *eventFlusher) Publish(stream string, event interface{}) {
	asJSON, err := jsoniter.ConfigFastest.Marshal(event)
	if err != nil {
		panic(err)
	}
	ef.mutex.Lock()
	defer ef.mutex.Unlock()
	if ef.events[stream] == nil {
		ef.events[stream] = []EventAsMap{{"_s": string(asJSON)}}
	} else {
		ef.events[stream] = append(ef.events[stream], EventAsMap{"_s": string(asJSON)})
	}
}

func (ef *eventFlusher) Flush() {
	ef.mutex.Lock()
	defer ef.mutex.Unlock()
	grouped := make(map[*RedisCache]map[string][]EventAsMap)
	for stream, events := range ef.events {
		r := getRedisForStream(ef.eb.engine, stream)
		if grouped[r] == nil {
			grouped[r] = make(map[string][]EventAsMap)
		}
		if grouped[r][stream] == nil {
			grouped[r][stream] = events
		} else {
			grouped[r][stream] = append(grouped[r][stream], events...)
		}
	}
	for r, events := range grouped {
		p := r.PipeLine()
		for stream, list := range events {
			for _, e := range list {
				var v map[string]interface{} = e
				p.XAdd(stream, v)
			}
		}
		p.Exec()
	}
	ef.events = make(map[string][]EventAsMap)
}

func (e *Engine) GetEventBroker() EventBroker {
	if e.eventBroker == nil {
		e.eventBroker = &eventBroker{engine: e}
	}
	return e.eventBroker
}

func (eb *eventBroker) NewFlusher() EventFlusher {
	return &eventFlusher{eb: eb, events: make(map[string][]EventAsMap)}
}

func (eb *eventBroker) PublishMap(stream string, event EventAsMap) (id string) {
	var v map[string]interface{} = event
	id = getRedisForStream(eb.engine, stream).xAdd(stream, v)
	return id
}

func (eb *eventBroker) Publish(stream string, event interface{}) (id string) {
	asJSON, err := jsoniter.ConfigFastest.Marshal(event)
	if err != nil {
		panic(err)
	}
	return eb.PublishMap(stream, EventAsMap{"_s": string(asJSON)})
}

func getRedisForStream(engine *Engine, stream string) *RedisCache {
	pool, has := engine.registry.redisStreamPools[stream]
	if !has {
		panic(fmt.Errorf("unregistered stream %s", stream))
	}
	return engine.GetRedis(pool)
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
	speedPrefixKey := group + "_" + redisPool
	return &eventsConsumer{redis: eb.engine.GetRedis(redisPool), name: name, streams: streams, group: group,
		loop: true, block: time.Second * 30, lockTTL: time.Second * 90, lockTick: time.Minute,
		garbageTick: time.Second * 30, minIdle: pendingClaimCheckDuration,
		claimDuration: pendingClaimCheckDuration, speedLimit: 10000, speedPrefixKey: speedPrefixKey}
}

type eventsConsumer struct {
	redis                 *RedisCache
	name                  string
	nr                    int
	speedPrefixKey        string
	deadConsumers         int
	speedEvents           int
	speedTimeMicroseconds int64
	speedLimit            int
	nrString              string
	streams               []string
	group                 string
	loop                  bool
	block                 time.Duration
	heartBeatTime         time.Time
	heartBeat             func()
	heartBeatDuration     time.Duration
	lockTTL               time.Duration
	lockTick              time.Duration
	garbageTick           time.Duration
	minIdle               time.Duration
	claimDuration         time.Duration
	garbageCollectorSha1  string
	consumed              int
	consumedMutex         sync.Mutex
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
	for {
		valid := r.consume(ctx, count, blocking, handler)
		if valid || !r.loop {
			break
		}
		time.Sleep(time.Second * 10)
	}
}

func (r *eventsConsumer) consume(ctx context.Context, count int, blocking bool, handler EventConsumerHandler) bool {
	uniqueLockKey := r.group + "_" + r.name + "_" + r.redis.code
	runningKey := uniqueLockKey + "_running"
	locker := r.redis.engine.GetLocker()
	nr := 0
	var lockName string
	var lock *Lock
	for {
		nr++
		lockName = uniqueLockKey + "-" + strconv.Itoa(nr)
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
		r.redis.HSet(runningKey, r.nrString, strconv.FormatInt(time.Now().Unix(), 10))
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
				r.redis.HSet(runningKey, r.nrString, strconv.FormatInt(now.Unix(), 10))
			}
		}
	}()
	if r.nr == 1 {
		garbageTicker := time.NewTicker(r.garbageTick)
		go func() {
			r.garbageCollector(true)
			for {
				select {
				case <-ctx.Done():
					return
				case <-done:
					return
				case <-garbageTicker.C:
					r.garbageCollector(false)
				}
			}
		}()
	}

	lastIDs := make(map[string]string)
	for _, stream := range r.streams {
		r.redis.XGroupCreateMkStream(stream, r.group, "0")
	}
	keys := []string{"pending", "0", ">"}
	streams := make([]string, len(r.streams)*2)
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
						end := strconv.FormatInt(time.Now().Add(-r.minIdle).UnixNano()/1000000, 10)
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
				for _, stream := range r.streams {
					lastIDs[stream] = "0"
				}
			}
			for {
				if canceled {
					return true
				}
				if !hasLock || time.Since(lockAcquired) > r.lockTTL {
					r.redis.engine.Log().Warn("consumer %s for group %s lost lock", nil)
					return false
				}
				i := 0
				for _, stream := range r.streams {
					streams[i] = stream
					i++
				}
				for _, stream := range r.streams {
					if invalidCheck {
						streams[i] = lastIDs[stream]
					} else {
						streams[i] = ">"
					}
					i++
				}
				a := &redis.XReadGroupArgs{Consumer: r.getName(), Group: r.group, Streams: streams, Count: int64(count), Block: b}
				results := r.redis.XReadGroup(a)
				if canceled {
					return true
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
					if r.loop && !blocking && normalCheck {
						time.Sleep(time.Second * 30)
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
				r.speedEvents += totalMessages
				r.consumedMutex.Lock()
				r.consumed += totalMessages
				r.consumedMutex.Unlock()
				start := time.Now()
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
				r.speedTimeMicroseconds += time.Since(start).Microseconds()
				if r.speedEvents >= r.speedLimit {
					today := time.Now().Format("01-02-06")
					key := speedHSetKey + today
					pipeline := r.redis.PipeLine()
					pipeline.Expire(key, time.Hour*168)
					pipeline.HIncrBy(key, r.speedPrefixKey+"e", int64(r.speedEvents))
					pipeline.HIncrBy(key, r.speedPrefixKey+"t", r.speedTimeMicroseconds)
					pipeline.Exec()
					r.speedEvents = 0
					r.speedTimeMicroseconds = 0
				}
				if r.deadConsumers > 0 && time.Since(pendingCheckedTime) >= r.claimDuration {
					break
				}
				if normalCheck && time.Since(started) > time.Minute*10 {
					break
				}
			}
		}
		if !r.loop {
			r.HeartBeat(true)
			break
		}
	}
	return true
}

func (r *eventsConsumer) getName() string {
	return r.name + "-" + r.nrString
}

func (r *eventsConsumer) incrementID(id string) string {
	s := strings.Split(id, "-")
	counter, _ := strconv.Atoi(s[1])
	return s[0] + "-" + strconv.Itoa(counter+1)
}

func (r *eventsConsumer) garbageCollector(force bool) {
	func() {
		if !force {
			r.consumedMutex.Lock()
			if r.consumed == 0 {
				r.consumedMutex.Unlock()
				return
			}
			r.consumed = 0
			r.consumedMutex.Unlock()
		}
		defer func() {
			if rec := recover(); rec != nil {
				r.redis.engine.reportError(rec)
			}
		}()

		def := r.redis.engine.registry.redisStreamGroups[r.redis.code]
		for _, stream := range r.streams {
			info := r.redis.XInfoGroups(stream)
			ids := make(map[string][]int64)
			for name := range def[stream] {
				ids[name] = []int64{0, 0}
			}
			inPending := false
			for _, group := range info {
				_, has := ids[group.Name]
				if !has {
					r.redis.engine.log.Warn("not registered stream group "+group.Name+" in stream"+stream, nil)
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
			var end string
			if inPending {
				if minID[1] > 0 {
					end = strconv.FormatInt(minID[0], 10) + "-" + strconv.FormatInt(minID[1]-1, 10)
				} else {
					end = strconv.FormatInt(minID[0]-1, 10)
				}
			} else {
				end = strconv.FormatInt(minID[0], 10) + "-" + strconv.FormatInt(minID[1], 10)
			}

			if r.garbageCollectorSha1 == "" {
				sha1, has := r.redis.Get("_orm_gc_sha1")
				if !has {
					script := `
						local count = 0
						local all = 0
						while(true)
						do
							local T = redis.call('XRANGE', KEYS[1], "-", ARGV[1], "COUNT", 1000)
							local ids = {}
							for _, v in pairs(T) do
								table.insert(ids, v[1])
								count = count + 1
							end
							if table.getn(ids) > 0 then
								redis.call('XDEL', KEYS[1], unpack(ids))
							end
							if table.getn(ids) < 1000 then
								all = 1
								break
							end
							if count >= 100000 then
								break
							end
						end
						return all
						`
					r.garbageCollectorSha1 = r.redis.ScriptLoad(script)
					r.redis.Set("_orm_gc_sha1", r.garbageCollectorSha1, 604800)
				} else {
					r.garbageCollectorSha1 = sha1
				}
			}

			for {
				res := r.redis.EvalSha(r.garbageCollectorSha1, []string{stream}, end)
				if res == int64(1) {
					break
				}
			}
		}
	}()
}
