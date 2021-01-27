package orm

import (
	"context"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redis_rate/v9"
)

type PubSub struct {
	pubSub          *redis.PubSub
	r               *RedisCache
	disableLoop     bool
	maxLoopDuration time.Duration
	heartBeat       func()
}

func (p *PubSub) DisableLoop() {
	p.disableLoop = true
}

func (p *PubSub) SetMaxLoopDuration(duration time.Duration) {
	p.maxLoopDuration = duration
}

func (p *PubSub) SetHeartBeat(beat func()) {
	p.heartBeat = beat
}

func (p *PubSub) Consume(size int, handler func(items []*redis.Message)) {
	delivery := p.pubSub.ChannelSize(size)
	counter := 0
	items := make([]*redis.Message, 0)
	beatTime := time.Now()
	loopTime := time.Now().UnixNano()
	for {
		now := time.Now()
		nowNano := now.UnixNano()
		timeOut := (nowNano - loopTime) >= p.maxLoopDuration.Nanoseconds()
		if counter > 0 && (timeOut || counter == size) {
			handler(items)
			items = nil
			loopTime = time.Now().UnixNano()
			counter = 0
			if p.disableLoop {
				if p.heartBeat != nil {
					p.heartBeat()
				}
				return
			}
		} else if timeOut && p.disableLoop {
			return
		}
		if p.heartBeat != nil && now.Sub(beatTime).Minutes() >= 1 {
			p.heartBeat()
			beatTime = now
		}
		select {
		case item := <-delivery:
			items = append(items, item)
			counter++
		case <-time.After(time.Second):
		}
	}
}

func (p *PubSub) Close() {
	start := time.Now()
	err := p.pubSub.Close()
	if p.r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		p.r.fillLogFields("[ORM][REDIS][CLOSE PUBSUB]", start, "closepubsub", -1, 1,
			map[string]interface{}{"Channels": p.pubSub.String()}, err)
	}
	checkError(err)
}

func (p *PubSub) Unsubscribe(channels ...string) {
	start := time.Now()
	err := p.pubSub.Unsubscribe(p.r.client.Context(), channels...)
	if p.r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		p.r.fillLogFields("[ORM][REDIS][UNSUSCRIBE PUBSUB]", start, "unsusgribe", -1, len(channels),
			map[string]interface{}{"Channels": channels}, err)
	}
	checkError(err)
}

func (p *PubSub) PUnsubscribe(channels ...string) {
	start := time.Now()
	err := p.pubSub.PUnsubscribe(p.r.client.Context(), channels...)
	if p.r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		p.r.fillLogFields("[ORM][REDIS][PUNSUSCRIBE PUBSUB]", start, "punsusgribe", -1, len(channels),
			map[string]interface{}{"Channels": channels}, err)
	}
	checkError(err)
}

func (p *PubSub) String() string {
	return p.pubSub.String()
}

type RedisCache struct {
	engine  *Engine
	ctx     context.Context
	code    string
	client  *redis.Client
	limiter *redis_rate.Limiter
}

type GetSetProvider func() interface{}

func (r *RedisCache) RateLimit(key string, limit redis_rate.Limit) bool {
	if r.limiter == nil {
		r.limiter = redis_rate.NewLimiter(r.client)
	}
	start := time.Now()
	res, err := r.limiter.Allow(r.client.Context(), key, limit)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][RATE_LIMIT]", start,
			"rate_limit", 0, 1, map[string]interface{}{"Key": key}, err)
	}
	checkError(err)
	return res.Allowed > 0
}

func (r *RedisCache) GetSet(key string, ttlSeconds int, provider GetSetProvider) interface{} {
	val, has := r.Get(key)
	if !has {
		userVal := provider()
		encoded, _ := jsoniter.ConfigFastest.Marshal(userVal)
		r.Set(key, string(encoded), ttlSeconds)
		return userVal
	}
	var data interface{}
	_ = jsoniter.ConfigFastest.Unmarshal([]byte(val), &data)
	return data
}

func (r *RedisCache) PipeLine() *RedisPipeLine {
	return &RedisPipeLine{ctx: r.client.Context(), pool: r.code, engine: r.engine, pipeLine: r.client.Pipeline()}
}

func (r *RedisCache) Info(section ...string) string {
	start := time.Now()
	val, err := r.client.Info(r.ctx, section...).Result()
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][INFO]", start, "info", 0, 1,
			map[string]interface{}{"section": section}, nil)
	}
	return val
}

func (r *RedisCache) Get(key string) (value string, has bool) {
	start := time.Now()
	val, err := r.client.Get(r.ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			err = nil
		}
		if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
			r.fillLogFields("[ORM][REDIS][GET]", start, "get", 1, 1, map[string]interface{}{"Key": key}, err)
		}
		checkError(err)
		return "", false
	}
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][GET]", start, "get", 0, 1, map[string]interface{}{"Key": key}, nil)
	}
	return val, true
}

func (r *RedisCache) Set(key string, value interface{}, ttlSeconds int) {
	start := time.Now()
	_, err := r.client.Set(r.ctx, key, value, time.Duration(ttlSeconds)*time.Second).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][SET]", start, "set", -1, 1,
			map[string]interface{}{"Key": key, "value": value, "ttl": ttlSeconds}, err)
	}
	checkError(err)
}

func (r *RedisCache) LPush(key string, values ...interface{}) int64 {
	start := time.Now()
	val, err := r.client.LPush(r.ctx, key, values...).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][LPUSH]", start, "lpush", -1, len(values),
			map[string]interface{}{"Key": key, "values": values}, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) RPush(key string, values ...interface{}) int64 {
	start := time.Now()
	val, err := r.client.RPush(r.ctx, key, values...).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][RPUSH]", start, "rpush", -1, len(values),
			map[string]interface{}{"Key": key, "values": values}, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) LLen(key string) int64 {
	start := time.Now()
	val, err := r.client.LLen(r.ctx, key).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][LLEN]", start, "llen", -1, 1,
			map[string]interface{}{"Key": key}, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) Exists(keys ...string) int64 {
	start := time.Now()
	val, err := r.client.Exists(r.ctx, keys...).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][EXISTS]", start, "exists", -1, 1,
			map[string]interface{}{"Keys": keys}, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) Type(key string) string {
	start := time.Now()
	val, err := r.client.Type(r.ctx, key).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][TYPE]", start, "type", -1, 1,
			map[string]interface{}{"Key": key}, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) LRange(key string, start, stop int64) []string {
	s := time.Now()
	val, err := r.client.LRange(r.ctx, key, start, stop).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][LRANGE]", s, "lrange", -1, len(val),
			map[string]interface{}{"Key": key, "start": start, "stop": stop}, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) LSet(key string, index int64, value interface{}) {
	start := time.Now()
	_, err := r.client.LSet(r.ctx, key, index, value).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][LSET]", start, "lset", -1, 1,
			map[string]interface{}{"Key": key, "index": index, "value": value}, err)
	}
	checkError(err)
}

func (r *RedisCache) RPop(key string) (value string, found bool) {
	start := time.Now()
	val, err := r.client.RPop(r.ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			err = nil
		}
		if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
			r.fillLogFields("[ORM][REDIS][RPOP]", start, "rpop", 1, 1,
				map[string]interface{}{"Key": key}, err)
		}
		checkError(err)
		return "", false
	}
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][RPOP]", start, "rpop", 0, 1,
			map[string]interface{}{"Key": key}, nil)
	}
	return val, true
}

func (r *RedisCache) LRem(key string, count int64, value interface{}) {
	start := time.Now()
	_, err := r.client.LRem(r.ctx, key, count, value).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][LREM]", start, "lrem", -1, 1,
			map[string]interface{}{"Key": key, "count": count, "value": value}, err)
	}
	checkError(err)
}

func (r *RedisCache) Ltrim(key string, start, stop int64) {
	s := time.Now()
	_, err := r.client.LTrim(r.ctx, key, start, stop).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][LTRIM]", s, "ltrim", -1, 1,
			map[string]interface{}{"Key": key, "start": start, "stop": stop}, err)
	}
	checkError(err)
}

func (r *RedisCache) HMset(key string, fields map[string]interface{}) {
	start := time.Now()
	_, err := r.client.HMSet(r.ctx, key, fields).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][HMSET]", start, "hmset", -1, len(fields),
			map[string]interface{}{"Key": key, "fields": fields}, err)
	}
	checkError(err)
}

func (r *RedisCache) HSet(key string, field string, value interface{}) {
	start := time.Now()
	_, err := r.client.HSet(r.ctx, key, field, value).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][HSET]", start, "hset", -1, 1,
			map[string]interface{}{"Key": key, "field": field, "value": value}, err)
	}
	checkError(err)
}

func (r *RedisCache) HDel(key string, fields ...string) {
	start := time.Now()
	_, err := r.client.HDel(r.ctx, key, fields...).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][HDEL]", start, "hdel", -1, len(fields),
			map[string]interface{}{"Key": key, "fields": fields}, err)
	}
	checkError(err)
}

func (r *RedisCache) HMget(key string, fields ...string) map[string]interface{} {
	start := time.Now()
	val, err := r.client.HMGet(r.ctx, key, fields...).Result()
	results := make(map[string]interface{}, len(fields))
	misses := 0
	for index, v := range val {
		if v == nil {
			misses++
		}
		results[fields[index]] = v
	}
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][HMGET]", start, "hmget", misses, len(fields),
			map[string]interface{}{"Key": key, "fields": fields}, err)
	}
	return results
}

func (r *RedisCache) HGetAll(key string) map[string]string {
	start := time.Now()
	val, err := r.client.HGetAll(r.ctx, key).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][HGETALL]", start, "hgetall", -1, 1,
			map[string]interface{}{"Key": key}, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) HGet(key, field string) (value string, has bool) {
	misses := 0
	start := time.Now()
	val, err := r.client.HGet(r.ctx, key, field).Result()
	if err == redis.Nil {
		err = nil
		misses = 1
	}
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][HGET]", start, "hget", misses, 1,
			map[string]interface{}{"Key": key, "field": field}, err)
	}
	checkError(err)
	return val, misses == 0
}

func (r *RedisCache) HIncrBy(key, field string, incr int64) int64 {
	start := time.Now()
	val, err := r.client.HIncrBy(r.ctx, key, field, incr).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][HINCRBY]", start, "hincrby", -1, 1,
			map[string]interface{}{"Key": key, "incr": incr}, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) ZAdd(key string, members ...*redis.Z) int64 {
	start := time.Now()
	val, err := r.client.ZAdd(r.ctx, key, members...).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][ZADD]", start, "zadd", -1, len(members),
			map[string]interface{}{"Key": key, "members": len(members)}, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) ZRevRange(key string, start, stop int64) []string {
	startTime := time.Now()
	val, err := r.client.ZRevRange(r.ctx, key, start, stop).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][ZREVRANGE]", startTime, "zrevrange", -1, 1,
			map[string]interface{}{"Key": key, "start": start, "stop": stop}, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) ZRevRangeWithScores(key string, start, stop int64) []redis.Z {
	startTime := time.Now()
	val, err := r.client.ZRevRangeWithScores(r.ctx, key, start, stop).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][ZREVRANGEWITHSCORES]", startTime, "zrevrangewithscores", -1, 1,
			map[string]interface{}{"Key": key, "start": start, "stop": stop}, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) ZRangeWithScores(key string, start, stop int64) []redis.Z {
	startTime := time.Now()
	val, err := r.client.ZRangeWithScores(r.ctx, key, start, stop).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][ZRANGEWITHSCORES]", startTime, "zrangewithscores", -1, 1,
			map[string]interface{}{"Key": key, "start": start, "stop": stop}, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) ZCard(key string) int64 {
	start := time.Now()
	val, err := r.client.ZCard(r.ctx, key).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][ZCARD]", start, "zcard", -1, 1,
			map[string]interface{}{"Key": key}, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) ZCount(key string, min, max string) int64 {
	start := time.Now()
	val, err := r.client.ZCount(r.ctx, key, min, max).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][ZCOUNT]", start, "zcount", -1, 1,
			map[string]interface{}{"Key": key, "min": min, "max": max}, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) ZScore(key, member string) float64 {
	start := time.Now()
	val, err := r.client.ZScore(r.ctx, key, member).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][ZSCORE]", start, "zscore", -1, 1,
			map[string]interface{}{"Key": key, "member": member}, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) MSet(pairs ...interface{}) {
	start := time.Now()
	_, err := r.client.MSet(r.ctx, pairs...).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][MSET]", start, "mset", -1, len(pairs),
			map[string]interface{}{"Pairs": pairs}, err)
	}
	checkError(err)
}

func (r *RedisCache) MGet(keys ...string) map[string]interface{} {
	start := time.Now()
	val, err := r.client.MGet(r.ctx, keys...).Result()
	results := make(map[string]interface{}, len(keys))
	misses := 0
	for index, v := range val {
		results[keys[index]] = v
		if v == nil {
			misses++
		}
	}
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][MGET]", start, "mget", misses, len(keys),
			map[string]interface{}{"Keys": keys}, err)
	}
	return results
}

func (r *RedisCache) SAdd(key string, members ...interface{}) int64 {
	start := time.Now()
	val, err := r.client.SAdd(r.ctx, key, members...).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][SADD]", start, "sadd", -1, len(members),
			map[string]interface{}{"Key": key, "members": len(members)}, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) SCard(key string) int64 {
	start := time.Now()
	val, err := r.client.SCard(r.ctx, key).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][SCARD]", start, "scard", -1, 1,
			map[string]interface{}{"Key": key}, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) SPop(key string) (string, bool) {
	start := time.Now()
	val, err := r.client.SPop(r.ctx, key).Result()
	found := true
	if err == redis.Nil {
		err = nil
		found = false
	}
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][SPOP]", start, "spop", -1, 1,
			map[string]interface{}{"Key": key}, err)
	}
	checkError(err)
	return val, found
}

func (r *RedisCache) SPopN(key string, max int64) []string {
	start := time.Now()
	val, err := r.client.SPopN(r.ctx, key, max).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][SPOPN]", start, "spopn", -1, 1,
			map[string]interface{}{"Key": key, "max": max}, err)
	}
	checkError(err)
	return val
}

func (r *RedisCache) Del(keys ...string) {
	start := time.Now()
	_, err := r.client.Del(r.ctx, keys...).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][DEL]", start, "del", -1, len(keys),
			map[string]interface{}{"Keys": keys}, err)
	}
	checkError(err)
}

func (r *RedisCache) PSubscribe(channels ...string) *PubSub {
	start := time.Now()
	pubSub := r.client.PSubscribe(r.ctx, channels...)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][PSUBSCRIBE]", start, "psubscribe", -1, len(channels),
			map[string]interface{}{"channels": channels}, nil)
	}
	return &PubSub{pubSub: pubSub, r: r}
}

func (r *RedisCache) Subscribe(channels ...string) *PubSub {
	start := time.Now()
	pubSub := r.client.Subscribe(r.ctx, channels...)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][SUBSCRIBE]", start, "subscribe", -1, len(channels),
			map[string]interface{}{"channels": channels}, nil)
	}
	return &PubSub{pubSub: pubSub, r: r}
}

func (r *RedisCache) Publish(channel string, message interface{}) {
	start := time.Now()
	_, err := r.client.Publish(r.ctx, channel, message).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][PUBLISH]", start, "publish", -1, 1,
			map[string]interface{}{"channel": channel, "message": message}, err)
	}
	checkError(err)
}

func (r *RedisCache) XTrim(key string, maxLen int64, approx bool) (deleted int64) {
	start := time.Now()
	var err error
	if approx {
		deleted, err = r.client.XTrimApprox(r.ctx, key, maxLen).Result()
	} else {
		deleted, err = r.client.XTrim(r.ctx, key, maxLen).Result()
	}
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XTRIM]", start, "xtrim", 0, 1,
			map[string]interface{}{"key": key, "max_len": maxLen, "approx": approx}, nil)
	}
	return deleted
}

func (r *RedisCache) XRange(stream, start, stop string, count int64) []redis.XMessage {
	s := time.Now()
	deleted, err := r.client.XRangeN(r.ctx, stream, start, stop, count).Result()
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XRANGE]", s, "xrange", 0, 1,
			map[string]interface{}{"stream": stream, "start": start, "stop": stop, "count": count}, nil)
	}
	return deleted
}

func (r *RedisCache) XRevRange(stream, start, stop string, count int64) []redis.XMessage {
	s := time.Now()
	deleted, err := r.client.XRevRangeN(r.ctx, stream, start, stop, count).Result()
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XREVRANGE]", s, "xrevrange", 0, 1,
			map[string]interface{}{"stream": stream, "start": start, "stop": stop, "count": count}, nil)
	}
	return deleted
}

func (r *RedisCache) XInfoStream(stream string) *redis.XInfoStream {
	start := time.Now()
	info, err := r.client.XInfoStream(r.ctx, stream).Result()
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XINFO]", start, "xinfo", 0, 1,
			map[string]interface{}{"stream": stream}, nil)
	}
	return info
}

func (r *RedisCache) XInfoGroups(stream string) []redis.XInfoGroup {
	start := time.Now()
	info, err := r.client.XInfoGroups(r.ctx, stream).Result()
	if err != nil && err.Error() == "ERR no such key" {
		return make([]redis.XInfoGroup, 0)
	}
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XINFO]", start, "xinfo", 0, 1,
			map[string]interface{}{"group": stream}, nil)
	}
	return info
}

func (r *RedisCache) XGroupCreate(stream, group, start string) (key string, exists bool) {
	s := time.Now()
	res, err := r.client.XGroupCreate(r.ctx, stream, group, start).Result()
	if err != nil && strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return "OK", true
	}
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XGROUP]", s, "xgroup", 0, 1,
			map[string]interface{}{"arg": "create", "stream": stream, "group": group, "start": start}, nil)
	}
	return res, false
}

func (r *RedisCache) XGroupCreateMkStream(stream, group, start string) (key string, exists bool) {
	s := time.Now()
	res, err := r.client.XGroupCreateMkStream(r.ctx, stream, group, start).Result()
	created := false
	if err != nil && strings.HasPrefix(err.Error(), "BUSYGROUP") {
		created = true
		err = nil
		res = "OK"
	}
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XGROUP]", s, "xgroup", 0, 1,
			map[string]interface{}{"arg": "create mkstream", "stream": stream, "group": group, "start": start}, nil)
	}
	return res, created
}

func (r *RedisCache) XGroupDestroy(stream, group string) int64 {
	start := time.Now()
	res, err := r.client.XGroupDestroy(r.ctx, stream, group).Result()
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XGROUP]", start, "xgroup", 0, 1,
			map[string]interface{}{"arg": "destroy", "stream": stream, "group": group}, nil)
	}
	return res
}

func (r *RedisCache) XRead(a *redis.XReadArgs) []redis.XStream {
	start := time.Now()
	info, err := r.client.XRead(r.ctx, a).Result()
	if err != redis.Nil {
		checkError(err)
	}
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XREAD]", start, "xread", 0, 1,
			map[string]interface{}{"arg": a}, nil)
	}
	return info
}

func (r *RedisCache) XDel(stream string, ids ...string) int64 {
	s := time.Now()
	deleted, err := r.client.XDel(r.ctx, stream, ids...).Result()
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XDEL]", s, "xdel", 0, len(ids),
			map[string]interface{}{"stream": stream, "ids": ids}, nil)
	}
	return deleted
}

func (r *RedisCache) XGroupDelConsumer(stream, group, consumer string) int64 {
	start := time.Now()
	deleted, err := r.client.XGroupDelConsumer(r.ctx, stream, group, consumer).Result()
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XDEL]", start, "XGROUP", 0, 1,
			map[string]interface{}{"stream": stream, "group": group, "consumer": "consumer", "action": "delete consumer"}, nil)
	}
	return deleted
}

func (r *RedisCache) XReadGroup(a *redis.XReadGroupArgs) (streams []redis.XStream) {
	start := time.Now()
	streams, err := r.client.XReadGroup(r.ctx, a).Result()
	if err != redis.Nil {
		checkError(err)
	}
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XREADGROUP]", start, "xreadgroup", 0, 1,
			map[string]interface{}{"consumer": a.Consumer, "group": a.Group, "count": a.Count, "block": a.Block,
				"noack": a.NoAck, "streams": a.Streams}, nil)
	}
	return streams
}

func (r *RedisCache) XPending(stream, group string) *redis.XPending {
	start := time.Now()
	res, err := r.client.XPending(r.ctx, stream, group).Result()
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XPENDING]", start, "xpending", 0, 1,
			map[string]interface{}{"stream": stream, "group": group}, nil)
	}
	return res
}

func (r *RedisCache) XPendingExt(a *redis.XPendingExtArgs) []redis.XPendingExt {
	start := time.Now()
	res, err := r.client.XPendingExt(r.ctx, a).Result()
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XPENDING]", start, "xpending", 0, 1,
			map[string]interface{}{"group": a.Group, "stream": a.Stream, "consumer": a.Consumer, "count": a.Count,
				"start": a.Start, "end": a.End}, nil)
	}
	return res
}

func (r *RedisCache) xAdd(stream string, values interface{}) (id string) {
	a := &redis.XAddArgs{Stream: stream, ID: "*", Values: values}
	start := time.Now()
	id, err := r.client.XAdd(r.ctx, a).Result()
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XADD]", start, "xtrim", 0, 1,
			map[string]interface{}{"stream": stream, "id": a.ID, "values": a.Values, "max_len_app": a.MaxLenApprox}, nil)
	}
	return id
}

func (r *RedisCache) XLen(stream string) int64 {
	start := time.Now()
	l, err := r.client.XLen(r.ctx, stream).Result()
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XLEN]", start, "xlen", 0, 1,
			map[string]interface{}{"stream": stream}, nil)
	}
	return l
}

func (r *RedisCache) XClaim(a *redis.XClaimArgs) []redis.XMessage {
	start := time.Now()
	res, err := r.client.XClaim(r.ctx, a).Result()
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XCLAIM]", start, "xclaim", 0, len(a.Messages),
			map[string]interface{}{"arg": a}, nil)
	}
	return res
}

func (r *RedisCache) XClaimJustID(a *redis.XClaimArgs) []string {
	start := time.Now()
	res, err := r.client.XClaimJustID(r.ctx, a).Result()
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XCLAIM]", start, "xclaim", 0, len(a.Messages),
			map[string]interface{}{"arg": a, "justid": true}, nil)
	}
	return res
}

func (r *RedisCache) XAck(stream, group string, ids ...string) int64 {
	start := time.Now()
	res, err := r.client.XAck(r.ctx, stream, group, ids...).Result()
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XACK]", start, "xack", 0, len(ids),
			map[string]interface{}{"stream": stream, group: group, "ids": ids}, nil)
	}
	return res
}

func (r *RedisCache) FlushDB() {
	start := time.Now()
	_, err := r.client.FlushDB(r.ctx).Result()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][FLUSHDB]", start, "flushdb", -1, 1, nil, err)
	}
	checkError(err)
}

func (r *RedisCache) fillLogFields(message string, start time.Time, operation string, misses int, keys int,
	fields map[string]interface{}, err error) {
	now := time.Now()
	stop := time.Since(start).Microseconds()
	e := r.engine.queryLoggers[QueryLoggerSourceRedis].log.
		WithField("microseconds", stop).
		WithField("operation", operation).
		WithField("pool", r.code).
		WithField("keys", keys).
		WithField("target", "redis").
		WithField("started", start.UnixNano()).
		WithField("finished", now.UnixNano())
	if misses >= 0 {
		e = e.WithField("misses", misses)
	}
	for k, v := range fields {
		e = e.WithField(k, v)
	}
	if err != nil {
		injectLogError(err, e).Error(message)
	} else {
		e.Info(message)
	}
}
