package orm

import (
	"context"
	"fmt"
	"strings"
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redis_rate/v9"
)

const counterRedisAll = "redis.all"
const counterRedisKeysSet = "redis.keysSet"
const counterRedisKeysGet = "redis.keysGet"

type redisClient interface {
	Get(key string) (string, error)
	LRange(key string, start, stop int64) ([]string, error)
	HMGet(key string, fields ...string) ([]interface{}, error)
	HGetAll(key string) (map[string]string, error)
	LPush(key string, values ...interface{}) (int64, error)
	RPush(key string, values ...interface{}) (int64, error)
	RPop(key string) (string, error)
	LSet(key string, index int64, value interface{}) (string, error)
	LRem(key string, count int64, value interface{}) (int64, error)
	LTrim(key string, start, stop int64) (string, error)
	ZCard(key string) (int64, error)
	SCard(key string) (int64, error)
	ZCount(key string, min, max string) (int64, error)
	ZScore(key, member string) (float64, error)
	ZRevRange(key string, start, stop int64) ([]string, error)
	ZRangeWithScores(key string, start, stop int64) ([]redis.Z, error)
	ZRevRangeWithScores(key string, start, stop int64) ([]redis.Z, error)
	SPop(key string) (string, error)
	SPopN(key string, max int64) ([]string, error)
	Exists(keys ...string) (int64, error)
	Type(key string) (string, error)
	LLen(key string) (int64, error)
	ZAdd(key string, members ...*redis.Z) (int64, error)
	SAdd(key string, members ...interface{}) (int64, error)
	HMSet(key string, fields map[string]interface{}) (bool, error)
	HSet(key string, field string, value interface{}) (int64, error)
	MGet(keys ...string) ([]interface{}, error)
	Set(key string, value interface{}, expiration time.Duration) error
	MSet(pairs ...interface{}) error
	Del(keys ...string) error
	PSubscribe(channels ...string) *redis.PubSub
	Subscribe(channels ...string) *redis.PubSub
	Publish(channel string, message interface{}) error
	XTrim(key string, maxLen int64, approx bool) (int64, error)
	XAdd(a *redis.XAddArgs) (string, error)
	XLen(stream string) (int64, error)
	XClaim(a *redis.XClaimArgs) ([]redis.XMessage, error)
	XClaimJustID(a *redis.XClaimArgs) ([]string, error)
	XAck(stream, group string, ids ...string) (int64, error)
	XDel(stream string, ids ...string) (int64, error)
	XGroupDelConsumer(stream, group, consumer string) (int64, error)
	XRange(stream, start, stop string, count int64) ([]redis.XMessage, error)
	XRevRange(stream, start, stop string, count int64) ([]redis.XMessage, error)
	XInfoStream(stream string) (*redis.XInfoStream, error)
	XInfoGroups(stream string) ([]redis.XInfoGroup, error)
	XGroupCreate(stream, group, start string) (string, error)
	XGroupCreateMkStream(stream, group, start string) (string, error)
	XGroupDestroy(stream, group string) (int64, error)
	XRead(a *redis.XReadArgs) ([]redis.XStream, error)
	XReadGroup(a *redis.XReadGroupArgs) ([]redis.XStream, error)
	XPending(stream, group string) (*redis.XPending, error)
	XPendingExt(a *redis.XPendingExtArgs) ([]redis.XPendingExt, error)
	FlushDB() error
	Context() context.Context
}

type standardRedisClient struct {
	client *redis.Client
	ring   *redis.Ring
}

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
			p.r.engine.dataDog.incrementCounter(counterRedisAll, 1)
			p.r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
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
	p.r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	p.r.engine.dataDog.incrementCounter(counterRedisKeysSet, 1)
	checkError(err)
}

func (p *PubSub) Unsubscribe(channels ...string) {
	start := time.Now()
	err := p.pubSub.Unsubscribe(p.r.client.Context(), channels...)
	if p.r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		p.r.fillLogFields("[ORM][REDIS][UNSUSCRIBE PUBSUB]", start, "unsusgribe", -1, len(channels),
			map[string]interface{}{"Channels": channels}, err)
	}
	p.r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	p.r.engine.dataDog.incrementCounter(counterRedisKeysSet, 1)
	checkError(err)
}

func (p *PubSub) PUnsubscribe(channels ...string) {
	start := time.Now()
	err := p.pubSub.PUnsubscribe(p.r.client.Context(), channels...)
	if p.r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		p.r.fillLogFields("[ORM][REDIS][PUNSUSCRIBE PUBSUB]", start, "punsusgribe", -1, len(channels),
			map[string]interface{}{"Channels": channels}, err)
	}
	p.r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	p.r.engine.dataDog.incrementCounter(counterRedisKeysSet, 1)
	checkError(err)
}

func (p *PubSub) String() string {
	return p.pubSub.String()
}

func (c *standardRedisClient) Get(key string) (string, error) {
	if c.ring != nil {
		return c.ring.Get(c.ring.Context(), key).Result()
	}
	return c.client.Get(c.client.Context(), key).Result()
}

func (c *standardRedisClient) LRange(key string, start, stop int64) ([]string, error) {
	if c.ring != nil {
		return c.ring.LRange(c.ring.Context(), key, start, stop).Result()
	}
	return c.client.LRange(c.client.Context(), key, start, stop).Result()
}

func (c *standardRedisClient) HMGet(key string, fields ...string) ([]interface{}, error) {
	if c.ring != nil {
		return c.ring.HMGet(c.ring.Context(), key, fields...).Result()
	}
	return c.client.HMGet(c.client.Context(), key, fields...).Result()
}

func (c *standardRedisClient) HGetAll(key string) (map[string]string, error) {
	if c.ring != nil {
		return c.ring.HGetAll(c.ring.Context(), key).Result()
	}
	return c.client.HGetAll(c.client.Context(), key).Result()
}

func (c *standardRedisClient) LPush(key string, values ...interface{}) (int64, error) {
	if c.ring != nil {
		return c.ring.LPush(c.ring.Context(), key, values...).Result()
	}
	return c.client.LPush(c.client.Context(), key, values...).Result()
}

func (c *standardRedisClient) RPush(key string, values ...interface{}) (int64, error) {
	if c.ring != nil {
		return c.ring.RPush(c.ring.Context(), key, values...).Result()
	}
	return c.client.RPush(c.client.Context(), key, values...).Result()
}

func (c *standardRedisClient) RPop(key string) (string, error) {
	if c.ring != nil {
		return c.ring.RPop(c.ring.Context(), key).Result()
	}
	return c.client.RPop(c.client.Context(), key).Result()
}

func (c *standardRedisClient) LSet(key string, index int64, value interface{}) (string, error) {
	if c.ring != nil {
		return c.ring.LSet(c.ring.Context(), key, index, value).Result()
	}
	return c.client.LSet(c.client.Context(), key, index, value).Result()
}

func (c *standardRedisClient) LRem(key string, count int64, value interface{}) (int64, error) {
	if c.ring != nil {
		return c.ring.LRem(c.ring.Context(), key, count, value).Result()
	}
	return c.client.LRem(c.client.Context(), key, count, value).Result()
}

func (c *standardRedisClient) LTrim(key string, start, stop int64) (string, error) {
	if c.ring != nil {
		return c.ring.LTrim(c.ring.Context(), key, start, stop).Result()
	}
	return c.client.LTrim(c.client.Context(), key, start, stop).Result()
}

func (c *standardRedisClient) ZCard(key string) (int64, error) {
	if c.ring != nil {
		return c.ring.ZCard(c.ring.Context(), key).Result()
	}
	return c.client.ZCard(c.client.Context(), key).Result()
}

func (c *standardRedisClient) SCard(key string) (int64, error) {
	if c.ring != nil {
		return c.ring.SCard(c.ring.Context(), key).Result()
	}
	return c.client.SCard(c.client.Context(), key).Result()
}

func (c *standardRedisClient) ZCount(key string, min, max string) (int64, error) {
	if c.ring != nil {
		return c.ring.ZCount(c.ring.Context(), key, min, max).Result()
	}
	return c.client.ZCount(c.client.Context(), key, min, max).Result()
}

func (c *standardRedisClient) ZScore(key, member string) (float64, error) {
	if c.ring != nil {
		return c.ring.ZScore(c.ring.Context(), key, member).Result()
	}
	return c.client.ZScore(c.client.Context(), key, member).Result()
}

func (c *standardRedisClient) ZRevRange(key string, start, stop int64) ([]string, error) {
	if c.ring != nil {
		return c.ring.ZRevRange(c.ring.Context(), key, start, stop).Result()
	}
	return c.client.ZRevRange(c.client.Context(), key, start, stop).Result()
}

func (c *standardRedisClient) ZRangeWithScores(key string, start, stop int64) ([]redis.Z, error) {
	if c.ring != nil {
		return c.ring.ZRangeWithScores(c.ring.Context(), key, start, stop).Result()
	}
	return c.client.ZRangeWithScores(c.client.Context(), key, start, stop).Result()
}

func (c *standardRedisClient) ZRevRangeWithScores(key string, start, stop int64) ([]redis.Z, error) {
	if c.ring != nil {
		return c.ring.ZRevRangeWithScores(c.ring.Context(), key, start, stop).Result()
	}
	return c.client.ZRevRangeWithScores(c.client.Context(), key, start, stop).Result()
}

func (c *standardRedisClient) SPop(key string) (string, error) {
	if c.ring != nil {
		return c.ring.SPop(c.ring.Context(), key).Result()
	}
	return c.client.SPop(c.client.Context(), key).Result()
}

func (c *standardRedisClient) SPopN(key string, max int64) ([]string, error) {
	if c.ring != nil {
		return c.ring.SPopN(c.ring.Context(), key, max).Result()
	}
	return c.client.SPopN(c.client.Context(), key, max).Result()
}

func (c *standardRedisClient) LLen(key string) (int64, error) {
	if c.ring != nil {
		return c.ring.LLen(c.ring.Context(), key).Result()
	}
	return c.client.LLen(c.client.Context(), key).Result()
}

func (c *standardRedisClient) ZAdd(key string, members ...*redis.Z) (int64, error) {
	if c.ring != nil {
		return c.ring.ZAdd(c.ring.Context(), key, members...).Result()
	}
	return c.client.ZAdd(c.client.Context(), key, members...).Result()
}

func (c *standardRedisClient) SAdd(key string, members ...interface{}) (int64, error) {
	if c.ring != nil {
		return c.ring.SAdd(c.ring.Context(), key, members...).Result()
	}
	return c.client.SAdd(c.client.Context(), key, members...).Result()
}

func (c *standardRedisClient) HMSet(key string, fields map[string]interface{}) (bool, error) {
	if c.ring != nil {
		return c.ring.HMSet(c.ring.Context(), key, fields).Result()
	}
	return c.client.HMSet(c.client.Context(), key, fields).Result()
}

func (c *standardRedisClient) HSet(key string, field string, value interface{}) (int64, error) {
	if c.ring != nil {
		return c.ring.HSet(c.ring.Context(), key, field, value).Result()
	}
	return c.client.HSet(c.client.Context(), key, field, value).Result()
}

func (c *standardRedisClient) MGet(keys ...string) ([]interface{}, error) {
	if c.ring != nil {
		return c.ring.MGet(c.ring.Context(), keys...).Result()
	}
	return c.client.MGet(c.client.Context(), keys...).Result()
}

func (c *standardRedisClient) Set(key string, value interface{}, expiration time.Duration) error {
	if c.ring != nil {
		return c.ring.Set(c.ring.Context(), key, value, expiration).Err()
	}
	return c.client.Set(c.client.Context(), key, value, expiration).Err()
}

func (c *standardRedisClient) MSet(pairs ...interface{}) error {
	if c.ring != nil {
		return c.ring.MSet(c.ring.Context(), pairs...).Err()
	}
	return c.client.MSet(c.client.Context(), pairs...).Err()
}

func (c *standardRedisClient) Del(keys ...string) error {
	if c.ring != nil {
		return c.ring.Del(c.ring.Context(), keys...).Err()
	}
	return c.client.Del(c.client.Context(), keys...).Err()
}

func (c *standardRedisClient) Exists(keys ...string) (int64, error) {
	if c.ring != nil {
		return c.ring.Exists(c.ring.Context(), keys...).Result()
	}
	return c.client.Exists(c.client.Context(), keys...).Result()
}

func (c *standardRedisClient) Type(key string) (string, error) {
	if c.ring != nil {
		return c.ring.Type(c.ring.Context(), key).Result()
	}
	return c.client.Type(c.client.Context(), key).Result()
}

func (c *standardRedisClient) PSubscribe(channels ...string) *redis.PubSub {
	if c.ring != nil {
		return c.ring.PSubscribe(c.ring.Context(), channels...)
	}
	return c.client.PSubscribe(c.client.Context(), channels...)
}

func (c *standardRedisClient) Subscribe(channels ...string) *redis.PubSub {
	if c.ring != nil {
		return c.ring.Subscribe(c.ring.Context(), channels...)
	}
	return c.client.Subscribe(c.client.Context(), channels...)
}

func (c *standardRedisClient) Publish(channel string, message interface{}) error {
	if c.ring != nil {
		return c.ring.Publish(c.ring.Context(), channel, message).Err()
	}
	return c.client.Publish(c.client.Context(), channel, message).Err()
}

func (c *standardRedisClient) XTrim(key string, maxLen int64, approx bool) (int64, error) {
	if c.ring != nil {
		if approx {
			return c.ring.XTrimApprox(c.ring.Context(), key, maxLen).Result()
		}
		return c.ring.XTrim(c.ring.Context(), key, maxLen).Result()
	}
	if approx {
		return c.client.XTrimApprox(c.client.Context(), key, maxLen).Result()
	}
	return c.client.XTrim(c.client.Context(), key, maxLen).Result()
}

func (c *standardRedisClient) XAdd(a *redis.XAddArgs) (string, error) {
	if c.ring != nil {
		return c.ring.XAdd(c.ring.Context(), a).Result()
	}
	return c.client.XAdd(c.client.Context(), a).Result()
}

func (c *standardRedisClient) XLen(stream string) (int64, error) {
	if c.ring != nil {
		return c.ring.XLen(c.ring.Context(), stream).Result()
	}
	return c.client.XLen(c.client.Context(), stream).Result()
}

func (c *standardRedisClient) XClaim(a *redis.XClaimArgs) ([]redis.XMessage, error) {
	if c.ring != nil {
		return c.ring.XClaim(c.ring.Context(), a).Result()
	}
	return c.client.XClaim(c.client.Context(), a).Result()
}

func (c *standardRedisClient) XClaimJustID(a *redis.XClaimArgs) ([]string, error) {
	if c.ring != nil {
		return c.ring.XClaimJustID(c.ring.Context(), a).Result()
	}
	return c.client.XClaimJustID(c.client.Context(), a).Result()
}

func (c *standardRedisClient) XAck(stream, group string, ids ...string) (int64, error) {
	if c.ring != nil {
		return c.ring.XAck(c.ring.Context(), stream, group, ids...).Result()
	}
	return c.client.XAck(c.client.Context(), stream, group, ids...).Result()
}

func (c *standardRedisClient) XRange(stream, start, stop string, count int64) ([]redis.XMessage, error) {
	if c.ring != nil {
		return c.ring.XRangeN(c.ring.Context(), stream, start, stop, count).Result()
	}
	return c.client.XRangeN(c.client.Context(), stream, start, stop, count).Result()
}

func (c *standardRedisClient) XRevRange(stream, start, stop string, count int64) ([]redis.XMessage, error) {
	if c.ring != nil {
		return c.ring.XRevRangeN(c.ring.Context(), stream, start, stop, count).Result()
	}
	return c.client.XRevRangeN(c.client.Context(), stream, start, stop, count).Result()
}

func (c *standardRedisClient) XInfoStream(stream string) (*redis.XInfoStream, error) {
	if c.ring != nil {
		return c.ring.XInfoStream(c.ring.Context(), stream).Result()
	}
	return c.client.XInfoStream(c.client.Context(), stream).Result()
}

func (c *standardRedisClient) XInfoGroups(stream string) ([]redis.XInfoGroup, error) {
	if c.ring != nil {
		return c.ring.XInfoGroups(c.ring.Context(), stream).Result()
	}
	return c.client.XInfoGroups(c.client.Context(), stream).Result()
}

func (c *standardRedisClient) XGroupCreate(stream, group, start string) (string, error) {
	if c.ring != nil {
		return c.ring.XGroupCreate(c.ring.Context(), stream, group, start).Result()
	}
	return c.client.XGroupCreate(c.client.Context(), stream, group, start).Result()
}

func (c *standardRedisClient) XGroupCreateMkStream(stream, group, start string) (string, error) {
	if c.ring != nil {
		return c.ring.XGroupCreateMkStream(c.ring.Context(), stream, group, start).Result()
	}
	return c.client.XGroupCreateMkStream(c.client.Context(), stream, group, start).Result()
}

func (c *standardRedisClient) XGroupDestroy(stream, group string) (int64, error) {
	if c.ring != nil {
		return c.ring.XGroupDestroy(c.ring.Context(), stream, group).Result()
	}
	return c.client.XGroupDestroy(c.client.Context(), stream, group).Result()
}

func (c *standardRedisClient) XRead(a *redis.XReadArgs) ([]redis.XStream, error) {
	if c.ring != nil {
		return c.ring.XRead(c.ring.Context(), a).Result()
	}
	return c.client.XRead(c.client.Context(), a).Result()
}

func (c *standardRedisClient) XDel(stream string, ids ...string) (int64, error) {
	if c.ring != nil {
		return c.ring.XDel(c.ring.Context(), stream, ids...).Result()
	}
	return c.client.XDel(c.client.Context(), stream, ids...).Result()
}

func (c *standardRedisClient) XGroupDelConsumer(stream, group, consumer string) (int64, error) {
	if c.ring != nil {
		return c.ring.XGroupDelConsumer(c.ring.Context(), stream, group, consumer).Result()
	}
	return c.client.XGroupDelConsumer(c.client.Context(), stream, group, consumer).Result()
}

func (c *standardRedisClient) XReadGroup(a *redis.XReadGroupArgs) ([]redis.XStream, error) {
	if c.ring != nil {
		return c.ring.XReadGroup(c.ring.Context(), a).Result()
	}
	return c.client.XReadGroup(c.client.Context(), a).Result()
}

func (c *standardRedisClient) XPending(stream, group string) (*redis.XPending, error) {
	if c.ring != nil {
		return c.ring.XPending(c.ring.Context(), stream, group).Result()
	}
	return c.client.XPending(c.client.Context(), stream, group).Result()
}

func (c *standardRedisClient) XPendingExt(a *redis.XPendingExtArgs) ([]redis.XPendingExt, error) {
	if c.ring != nil {
		return c.ring.XPendingExt(c.ring.Context(), a).Result()
	}
	return c.client.XPendingExt(c.client.Context(), a).Result()
}

func (c *standardRedisClient) FlushDB() error {
	if c.ring != nil {
		return c.ring.FlushDB(c.ring.Context()).Err()
	}
	return c.client.FlushDB(c.client.Context()).Err()
}

func (c *standardRedisClient) Context() context.Context {
	if c.ring != nil {
		return c.ring.Context()
	}
	return c.client.Context()
}

type RedisCache struct {
	engine  *Engine
	code    string
	client  redisClient
	limiter *redis_rate.Limiter
}

type GetSetProvider func() interface{}

func (r *RedisCache) RateLimit(key string, limit redis_rate.Limit) bool {
	if r.limiter == nil {
		c := r.client.(*standardRedisClient)
		if c.client != nil {
			r.limiter = redis_rate.NewLimiter(c.client)
		} else {
			r.limiter = redis_rate.NewLimiter(c.ring)
		}
	}
	start := time.Now()
	res, err := r.limiter.Allow(r.client.Context(), key, limit)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][RATE_LIMIT]", start,
			"rate_limit", 0, 1, map[string]interface{}{"Key": key}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysSet, 1)
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

func (r *RedisCache) Get(key string) (value string, has bool) {
	start := time.Now()
	val, err := r.client.Get(key)
	if err != nil {
		if err == redis.Nil {
			err = nil
		}
		if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
			r.fillLogFields("[ORM][REDIS][GET]", start, "get", 1, 1, map[string]interface{}{"Key": key}, err)
		}
		r.engine.dataDog.incrementCounter(counterRedisAll, 1)
		r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
		checkError(err)
		return "", false
	}
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][GET]", start, "get", 0, 1, map[string]interface{}{"Key": key}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	return val, true
}

func (r *RedisCache) Set(key string, value interface{}, ttlSeconds int) {
	start := time.Now()
	err := r.client.Set(key, value, time.Duration(ttlSeconds)*time.Second)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][SET]", start, "set", -1, 1,
			map[string]interface{}{"Key": key, "value": value, "ttl": ttlSeconds}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysSet, 1)
	checkError(err)
}

func (r *RedisCache) LPush(key string, values ...interface{}) int64 {
	start := time.Now()
	val, err := r.client.LPush(key, values...)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][LPUSH]", start, "lpush", -1, len(values),
			map[string]interface{}{"Key": key, "values": values}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysSet, uint(len(values)))
	checkError(err)
	return val
}

func (r *RedisCache) RPush(key string, values ...interface{}) int64 {
	start := time.Now()
	val, err := r.client.RPush(key, values...)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][RPUSH]", start, "rpush", -1, len(values),
			map[string]interface{}{"Key": key, "values": values}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysSet, uint(len(values)))
	checkError(err)
	return val
}

func (r *RedisCache) LLen(key string) int64 {
	start := time.Now()
	val, err := r.client.LLen(key)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][LLEN]", start, "llen", -1, 1,
			map[string]interface{}{"Key": key}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	checkError(err)
	return val
}

func (r *RedisCache) Exists(keys ...string) int64 {
	start := time.Now()
	val, err := r.client.Exists(keys...)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][EXISTS]", start, "exists", -1, 1,
			map[string]interface{}{"Keys": keys}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	checkError(err)
	return val
}

func (r *RedisCache) Type(key string) string {
	start := time.Now()
	val, err := r.client.Type(key)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][TYPE]", start, "type", -1, 1,
			map[string]interface{}{"Key": key}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	checkError(err)
	return val
}

func (r *RedisCache) LRange(key string, start, stop int64) []string {
	s := time.Now()
	val, err := r.client.LRange(key, start, stop)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][LRANGE]", s, "lrange", -1, len(val),
			map[string]interface{}{"Key": key, "start": start, "stop": stop}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	checkError(err)
	return val
}

func (r *RedisCache) LSet(key string, index int64, value interface{}) {
	start := time.Now()
	_, err := r.client.LSet(key, index, value)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][LSET]", start, "lset", -1, 1,
			map[string]interface{}{"Key": key, "index": index, "value": value}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysSet, 1)
	checkError(err)
}

func (r *RedisCache) RPop(key string) (value string, found bool) {
	start := time.Now()
	val, err := r.client.RPop(key)
	if err != nil {
		if err == redis.Nil {
			err = nil
		}
		if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
			r.fillLogFields("[ORM][REDIS][RPOP]", start, "rpop", 1, 1,
				map[string]interface{}{"Key": key}, err)
		}
		r.engine.dataDog.incrementCounter(counterRedisAll, 1)
		r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
		checkError(err)
		return "", false
	}
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][RPOP]", start, "rpop", 0, 1,
			map[string]interface{}{"Key": key}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	return val, true
}

func (r *RedisCache) LRem(key string, count int64, value interface{}) {
	start := time.Now()
	_, err := r.client.LRem(key, count, value)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][LREM]", start, "lrem", -1, 1,
			map[string]interface{}{"Key": key, "count": count, "value": value}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	checkError(err)
}

func (r *RedisCache) Ltrim(key string, start, stop int64) {
	s := time.Now()
	_, err := r.client.LTrim(key, start, stop)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][LTRIM]", s, "ltrim", -1, 1,
			map[string]interface{}{"Key": key, "start": start, "stop": stop}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	checkError(err)
}

func (r *RedisCache) HMset(key string, fields map[string]interface{}) {
	start := time.Now()
	_, err := r.client.HMSet(key, fields)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][HMSET]", start, "hmset", -1, len(fields),
			map[string]interface{}{"Key": key, "fields": fields}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysSet, uint(len(fields)))
	checkError(err)
}

func (r *RedisCache) HSet(key string, field string, value interface{}) {
	start := time.Now()
	_, err := r.client.HSet(key, field, value)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][HSET]", start, "hset", -1, 1,
			map[string]interface{}{"Key": key, "field": field, "value": value}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysSet, 1)
	checkError(err)
}

func (r *RedisCache) HMget(key string, fields ...string) map[string]interface{} {
	start := time.Now()
	val, err := r.client.HMGet(key, fields...)
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, uint(len(fields)))
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
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, uint(len(fields)))
	return results
}

func (r *RedisCache) HGetAll(key string) map[string]string {
	start := time.Now()
	val, err := r.client.HGetAll(key)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][HGETALL]", start, "hgetall", -1, 1,
			map[string]interface{}{"Key": key}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	checkError(err)
	return val
}

func (r *RedisCache) ZAdd(key string, members ...*redis.Z) int64 {
	start := time.Now()
	val, err := r.client.ZAdd(key, members...)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][ZADD]", start, "zadd", -1, len(members),
			map[string]interface{}{"Key": key, "members": len(members)}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysSet, uint(len(members)))
	checkError(err)
	return val
}

func (r *RedisCache) ZRevRange(key string, start, stop int64) []string {
	startTime := time.Now()
	val, err := r.client.ZRevRange(key, start, stop)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][ZREVRANGE]", startTime, "zrevrange", -1, 1,
			map[string]interface{}{"Key": key, "start": start, "stop": stop}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	checkError(err)
	return val
}

func (r *RedisCache) ZRevRangeWithScores(key string, start, stop int64) []redis.Z {
	startTime := time.Now()
	val, err := r.client.ZRevRangeWithScores(key, start, stop)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][ZREVRANGEWITHSCORES]", startTime, "zrevrangewithscores", -1, 1,
			map[string]interface{}{"Key": key, "start": start, "stop": stop}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	checkError(err)
	return val
}

func (r *RedisCache) ZRangeWithScores(key string, start, stop int64) []redis.Z {
	startTime := time.Now()
	val, err := r.client.ZRangeWithScores(key, start, stop)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][ZRANGEWITHSCORES]", startTime, "zrangewithscores", -1, 1,
			map[string]interface{}{"Key": key, "start": start, "stop": stop}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	checkError(err)
	return val
}

func (r *RedisCache) ZCard(key string) int64 {
	start := time.Now()
	val, err := r.client.ZCard(key)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][ZCARD]", start, "zcard", -1, 1,
			map[string]interface{}{"Key": key}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	checkError(err)
	return val
}

func (r *RedisCache) ZCount(key string, min, max string) int64 {
	start := time.Now()
	val, err := r.client.ZCount(key, min, max)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][ZCOUNT]", start, "zcount", -1, 1,
			map[string]interface{}{"Key": key, "min": min, "max": max}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	checkError(err)
	return val
}

func (r *RedisCache) ZScore(key, member string) float64 {
	start := time.Now()
	val, err := r.client.ZScore(key, member)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][ZSCORE]", start, "zscore", -1, 1,
			map[string]interface{}{"Key": key, "member": member}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	checkError(err)
	return val
}

func (r *RedisCache) MSet(pairs ...interface{}) {
	start := time.Now()
	err := r.client.MSet(pairs...)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][MSET]", start, "mset", -1, len(pairs),
			map[string]interface{}{"Pairs": pairs}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysSet, uint(len(pairs)))
	checkError(err)
}

func (r *RedisCache) MGet(keys ...string) map[string]interface{} {
	start := time.Now()
	val, err := r.client.MGet(keys...)
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
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, uint(len(keys)))
	return results
}

func (r *RedisCache) SAdd(key string, members ...interface{}) int64 {
	start := time.Now()
	val, err := r.client.SAdd(key, members...)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][SADD]", start, "sadd", -1, len(members),
			map[string]interface{}{"Key": key, "members": len(members)}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysSet, uint(len(members)))
	checkError(err)
	return val
}

func (r *RedisCache) SCard(key string) int64 {
	start := time.Now()
	val, err := r.client.SCard(key)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][SCARD]", start, "scard", -1, 1,
			map[string]interface{}{"Key": key}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	checkError(err)
	return val
}

func (r *RedisCache) SPop(key string) (string, bool) {
	start := time.Now()
	val, err := r.client.SPop(key)
	found := true
	if err == redis.Nil {
		err = nil
		found = false
	}
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][SPOP]", start, "spop", -1, 1,
			map[string]interface{}{"Key": key}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	checkError(err)
	return val, found
}

func (r *RedisCache) SPopN(key string, max int64) []string {
	start := time.Now()
	val, err := r.client.SPopN(key, max)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][SPOPN]", start, "spopn", -1, 1,
			map[string]interface{}{"Key": key, "max": max}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	checkError(err)
	return val
}

func (r *RedisCache) Del(keys ...string) {
	start := time.Now()
	err := r.client.Del(keys...)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][DEL]", start, "del", -1, len(keys),
			map[string]interface{}{"Keys": keys}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, uint(len(keys)))
	checkError(err)
}

func (r *RedisCache) PSubscribe(channels ...string) *PubSub {
	start := time.Now()
	pubSub := r.client.PSubscribe(channels...)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][PSUBSCRIBE]", start, "psubscribe", -1, len(channels),
			map[string]interface{}{"channels": channels}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	return &PubSub{pubSub: pubSub, r: r}
}

func (r *RedisCache) Subscribe(channels ...string) *PubSub {
	start := time.Now()
	pubSub := r.client.Subscribe(channels...)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][SUBSCRIBE]", start, "subscribe", -1, len(channels),
			map[string]interface{}{"channels": channels}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	return &PubSub{pubSub: pubSub, r: r}
}

func (r *RedisCache) Publish(channel string, message interface{}) {
	start := time.Now()
	err := r.client.Publish(channel, message)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][PUBLISH]", start, "publish", -1, 1,
			map[string]interface{}{"channel": channel, "message": message}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	checkError(err)
}

func (r *RedisCache) XTrim(key string, maxLen int64, approx bool) (deleted int64) {
	start := time.Now()
	deleted, err := r.client.XTrim(key, maxLen, approx)
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XTRIM]", start, "xtrim", 0, 1,
			map[string]interface{}{"key": key, "max_len": maxLen, "approx": approx}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	return deleted
}

func (r *RedisCache) XRange(stream, start, stop string, count int64) []redis.XMessage {
	s := time.Now()
	deleted, err := r.client.XRange(stream, start, stop, count)
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XRANGE]", s, "xrange", 0, 1,
			map[string]interface{}{"stream": stream, "start": start, "stop": stop, "count": count}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	return deleted
}

func (r *RedisCache) XRevRange(stream, start, stop string, count int64) []redis.XMessage {
	s := time.Now()
	deleted, err := r.client.XRevRange(stream, start, stop, count)
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XREVRANGE]", s, "xrevrange", 0, 1,
			map[string]interface{}{"stream": stream, "start": start, "stop": stop, "count": count}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	return deleted
}

func (r *RedisCache) XInfoStream(stream string) *redis.XInfoStream {
	start := time.Now()
	info, err := r.client.XInfoStream(stream)
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XINFO]", start, "xinfo", 0, 1,
			map[string]interface{}{"stream": stream}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	return info
}

func (r *RedisCache) XInfoGroups(stream string) []redis.XInfoGroup {
	start := time.Now()
	info, err := r.client.XInfoGroups(stream)
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XINFO]", start, "xinfo", 0, 1,
			map[string]interface{}{"group": stream}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	return info
}

func (r *RedisCache) XGroupCreate(stream, group, start string) (key string, exists bool) {
	s := time.Now()
	res, err := r.client.XGroupCreate(stream, group, start)
	if err != nil && strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return "OK", true
	}
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XGROUP]", s, "xgroup", 0, 1,
			map[string]interface{}{"arg": "create", "stream": stream, "group": group, "start": start}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysSet, 1)
	return res, false
}

func (r *RedisCache) XGroupCreateMkStream(stream, group, start string) (key string, exists bool) {
	s := time.Now()
	res, err := r.client.XGroupCreateMkStream(stream, group, start)
	if err != nil && strings.HasPrefix(err.Error(), "BUSYGROUP") {
		return "OK", true
	}
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XGROUP]", s, "xgroup", 0, 1,
			map[string]interface{}{"arg": "create mkstream", "stream": stream, "group": group, "start": start}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysSet, 1)
	return res, false
}

func (r *RedisCache) XGroupDestroy(stream, group string) int64 {
	start := time.Now()
	res, err := r.client.XGroupDestroy(stream, group)
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XGROUP]", start, "xgroup", 0, 1,
			map[string]interface{}{"arg": "destroy", "stream": stream, "group": group}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysSet, 1)
	return res
}

func (r *RedisCache) XRead(a *redis.XReadArgs) []redis.XStream {
	start := time.Now()
	info, err := r.client.XRead(a)
	if err != redis.Nil {
		checkError(err)
	}
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XREAD]", start, "xread", 0, 1,
			map[string]interface{}{"arg": a}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	return info
}

func (r *RedisCache) XDel(stream string, ids ...string) int64 {
	s := time.Now()
	deleted, err := r.client.XDel(stream, ids...)
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XDEL]", s, "xdel", 0, len(ids),
			map[string]interface{}{"stream": stream, "ids": ids}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysSet, 1)
	return deleted
}

func (r *RedisCache) XGroupDelConsumer(stream, group, consumer string) int64 {
	start := time.Now()
	deleted, err := r.client.XGroupDelConsumer(stream, group, consumer)
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XDEL]", start, "XGROUP", 0, 1,
			map[string]interface{}{"stream": stream, "group": group, "consumer": "consumer", "action": "delete consumer"}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysSet, 1)
	return deleted
}

func (r *RedisCache) XReadGroup(a *redis.XReadGroupArgs) (streams []redis.XStream) {
	start := time.Now()
	streams, err := r.client.XReadGroup(a)
	if err != redis.Nil {
		checkError(err)
	}
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XREADGROUP]", start, "xreadgroup", 0, 1,
			map[string]interface{}{"consumer": a.Consumer, "group": a.Group, "count": a.Count, "block": a.Block,
				"noack": a.NoAck, "streams": a.Streams}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	return streams
}

func (r *RedisCache) XPending(stream, group string) *redis.XPending {
	start := time.Now()
	res, err := r.client.XPending(stream, group)
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XPENDING]", start, "xpending", 0, 1,
			map[string]interface{}{"stream": stream, "group": group}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	return res
}

func (r *RedisCache) XPendingExt(a *redis.XPendingExtArgs) []redis.XPendingExt {
	start := time.Now()
	res, err := r.client.XPendingExt(a)
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XPENDING]", start, "xpending", 0, 1,
			map[string]interface{}{"arg": a}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	return res
}

// values in the following formats:XAdd
//   - []interface{}{"key1", "value1", "key2", "value2"}
//   - []string("key1", "value1", "key2", "value2")
//   - map[string]interface{}{"key1": "value1", "key2": "value2"}
//
func (r *RedisCache) XAdd(stream string, values interface{}) (id string) {
	max, has := r.engine.registry.redisChannels[r.code][stream]
	if !has {
		panic(fmt.Errorf("unregistered channel %s in redis pool %s", stream, r.code))
	}
	a := &redis.XAddArgs{Stream: stream, ID: "*", Values: values}
	if max > 0 {
		a.MaxLen = int64(max)
	}
	start := time.Now()
	id, err := r.client.XAdd(a)
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XADD]", start, "xtrim", 0, 1,
			map[string]interface{}{"stream": stream, "id": a.ID, "values": a.Values, "max_len_app": a.MaxLenApprox}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysSet, 1)
	return id
}

func (r *RedisCache) XLen(stream string) int64 {
	start := time.Now()
	l, err := r.client.XLen(stream)
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XLEN]", start, "xlen", 0, 1,
			map[string]interface{}{"stream": stream}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	return l
}

func (r *RedisCache) XClaim(a *redis.XClaimArgs) []redis.XMessage {
	start := time.Now()
	res, err := r.client.XClaim(a)
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XCLAIM]", start, "xclaim", 0, len(a.Messages),
			map[string]interface{}{"arg": a}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	return res
}

func (r *RedisCache) XClaimJustID(a *redis.XClaimArgs) []string {
	start := time.Now()
	res, err := r.client.XClaimJustID(a)
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XCLAIM]", start, "xclaim", 0, len(a.Messages),
			map[string]interface{}{"arg": a, "justid": true}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	return res
}

func (r *RedisCache) XAck(stream, group string, ids ...string) int64 {
	start := time.Now()
	res, err := r.client.XAck(stream, group, ids...)
	checkError(err)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][XACK]", start, "xack", 0, len(ids),
			map[string]interface{}{"stream": stream, group: group, "ids": ids}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	return res
}

func (r *RedisCache) FlushDB() {
	start := time.Now()
	err := r.client.FlushDB()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][FLUSHDB]", start, "flushdb", -1, 1, nil, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
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
