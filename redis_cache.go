package orm

import (
	"time"

	jsoniter "github.com/json-iterator/go"

	"github.com/go-redis/redis/v7"
	"github.com/go-redis/redis_rate/v8"
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
	SPop(key string) (string, error)
	SPopN(key string, max int64) ([]string, error)
	LLen(key string) (int64, error)
	ZAdd(key string, members ...*redis.Z) (int64, error)
	SAdd(key string, members ...interface{}) (int64, error)
	HMSet(key string, fields map[string]interface{}) (bool, error)
	HSet(key string, field string, value interface{}) (int64, error)
	MGet(keys ...string) ([]interface{}, error)
	Set(key string, value interface{}, expiration time.Duration) error
	MSet(pairs ...interface{}) error
	Del(keys ...string) error
	FlushDB() error
}

type standardRedisClient struct {
	client *redis.Client
	ring   *redis.Ring
}

func (c *standardRedisClient) Get(key string) (string, error) {
	if c.ring != nil {
		return c.ring.Get(key).Result()
	}
	return c.client.Get(key).Result()
}

func (c *standardRedisClient) LRange(key string, start, stop int64) ([]string, error) {
	if c.ring != nil {
		return c.ring.LRange(key, start, stop).Result()
	}
	return c.client.LRange(key, start, stop).Result()
}

func (c *standardRedisClient) HMGet(key string, fields ...string) ([]interface{}, error) {
	if c.ring != nil {
		return c.ring.HMGet(key, fields...).Result()
	}
	return c.client.HMGet(key, fields...).Result()
}

func (c *standardRedisClient) HGetAll(key string) (map[string]string, error) {
	if c.ring != nil {
		return c.ring.HGetAll(key).Result()
	}
	return c.client.HGetAll(key).Result()
}

func (c *standardRedisClient) LPush(key string, values ...interface{}) (int64, error) {
	if c.ring != nil {
		return c.ring.LPush(key, values...).Result()
	}
	return c.client.LPush(key, values...).Result()
}

func (c *standardRedisClient) RPush(key string, values ...interface{}) (int64, error) {
	if c.ring != nil {
		return c.ring.RPush(key, values...).Result()
	}
	return c.client.RPush(key, values...).Result()
}

func (c *standardRedisClient) RPop(key string) (string, error) {
	if c.ring != nil {
		return c.ring.RPop(key).Result()
	}
	return c.client.RPop(key).Result()
}

func (c *standardRedisClient) LSet(key string, index int64, value interface{}) (string, error) {
	if c.ring != nil {
		return c.ring.LSet(key, index, value).Result()
	}
	return c.client.LSet(key, index, value).Result()
}

func (c *standardRedisClient) LRem(key string, count int64, value interface{}) (int64, error) {
	if c.ring != nil {
		return c.ring.LRem(key, count, value).Result()
	}
	return c.client.LRem(key, count, value).Result()
}

func (c *standardRedisClient) LTrim(key string, start, stop int64) (string, error) {
	if c.ring != nil {
		return c.ring.LTrim(key, start, stop).Result()
	}
	return c.client.LTrim(key, start, stop).Result()
}

func (c *standardRedisClient) ZCard(key string) (int64, error) {
	if c.ring != nil {
		return c.ring.ZCard(key).Result()
	}
	return c.client.ZCard(key).Result()
}

func (c *standardRedisClient) SCard(key string) (int64, error) {
	if c.ring != nil {
		return c.ring.SCard(key).Result()
	}
	return c.client.SCard(key).Result()
}

func (c *standardRedisClient) ZCount(key string, min, max string) (int64, error) {
	if c.ring != nil {
		return c.ring.ZCount(key, min, max).Result()
	}
	return c.client.ZCount(key, min, max).Result()
}

func (c *standardRedisClient) SPop(key string) (string, error) {
	if c.ring != nil {
		return c.ring.SPop(key).Result()
	}
	return c.client.SPop(key).Result()
}

func (c *standardRedisClient) SPopN(key string, max int64) ([]string, error) {
	if c.ring != nil {
		return c.ring.SPopN(key, max).Result()
	}
	return c.client.SPopN(key, max).Result()
}

func (c *standardRedisClient) LLen(key string) (int64, error) {
	if c.ring != nil {
		return c.ring.LLen(key).Result()
	}
	return c.client.LLen(key).Result()
}

func (c *standardRedisClient) ZAdd(key string, members ...*redis.Z) (int64, error) {
	if c.ring != nil {
		return c.ring.ZAdd(key, members...).Result()
	}
	return c.client.ZAdd(key, members...).Result()
}

func (c *standardRedisClient) SAdd(key string, members ...interface{}) (int64, error) {
	if c.ring != nil {
		return c.ring.SAdd(key, members...).Result()
	}
	return c.client.SAdd(key, members...).Result()
}

func (c *standardRedisClient) HMSet(key string, fields map[string]interface{}) (bool, error) {
	if c.ring != nil {
		return c.ring.HMSet(key, fields).Result()
	}
	return c.client.HMSet(key, fields).Result()
}

func (c *standardRedisClient) HSet(key string, field string, value interface{}) (int64, error) {
	if c.ring != nil {
		return c.ring.HSet(key, field, value).Result()
	}
	return c.client.HSet(key, field, value).Result()
}

func (c *standardRedisClient) MGet(keys ...string) ([]interface{}, error) {
	if c.ring != nil {
		return c.ring.MGet(keys...).Result()
	}
	return c.client.MGet(keys...).Result()
}

func (c *standardRedisClient) Set(key string, value interface{}, expiration time.Duration) error {
	if c.ring != nil {
		return c.ring.Set(key, value, expiration).Err()
	}
	return c.client.Set(key, value, expiration).Err()
}

func (c *standardRedisClient) MSet(pairs ...interface{}) error {
	if c.ring != nil {
		return c.ring.MSet(pairs...).Err()
	}
	return c.client.MSet(pairs...).Err()
}

func (c *standardRedisClient) Del(keys ...string) error {
	if c.ring != nil {
		return c.ring.Del(keys...).Err()
	}
	return c.client.Del(keys...).Err()
}

func (c *standardRedisClient) FlushDB() error {
	if c.ring != nil {
		return c.ring.FlushDB().Err()
	}
	return c.client.FlushDB().Err()
}

type RedisCache struct {
	engine  *Engine
	code    string
	client  redisClient
	limiter *redis_rate.Limiter
}

type GetSetProvider func() interface{}

func (r *RedisCache) RateLimit(key string, limit *redis_rate.Limit) bool {
	if r.limiter == nil {
		c := r.client.(*standardRedisClient)
		if c.client != nil {
			r.limiter = redis_rate.NewLimiter(c.client)
		} else {
			r.limiter = redis_rate.NewLimiter(c.ring)
		}
	}
	res, err := r.limiter.Allow(key, limit)
	if err != nil {
		panic(err)
	}
	return res.Allowed
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
			if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
				r.fillLogFields("[ORM][REDIS][GET]", start,
					"get", 1, 1, map[string]interface{}{"Key": key}, nil)
			}
			r.engine.dataDog.incrementCounter(counterRedisAll, 1)
			r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
			return "", false
		}
		if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
			r.fillLogFields("[ORM][REDIS][GET]", start, "get", 1, 1, map[string]interface{}{"Key": key}, err)
		}
		r.engine.dataDog.incrementCounter(counterRedisAll, 1)
		r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
		if err != nil {
			panic(err)
		}
		return "", false
	}
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][GET]", start, "get", 0, 1, map[string]interface{}{"Key": key}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	return val, true
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
	if err != nil {
		panic(err)
	}
	return val
}

func (r *RedisCache) HMget(key string, fields ...string) map[string]interface{} {
	start := time.Now()
	val, err := r.client.HMGet(key, fields...)
	if err != nil {
		if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
			r.fillLogFields("[ORM][REDIS][HMGET]", start, "hmget", 0, len(fields),
				map[string]interface{}{"Key": key, "fields": fields}, err)
		}
		panic(err)
	}
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
			map[string]interface{}{"Key": key, "fields": fields}, nil)
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
	if err != nil {
		panic(err)
	}
	return val
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
	if err != nil {
		panic(err)
	}
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
	if err != nil {
		panic(err)
	}
	return val
}

func (r *RedisCache) RPop(key string) (value string, found bool) {
	start := time.Now()
	val, err := r.client.RPop(key)
	if err != nil {
		if err == redis.Nil {
			if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
				r.fillLogFields("[ORM][REDIS][RPOP]", start, "rpop", 1, 1,
					map[string]interface{}{"Key": key}, nil)
			}
			r.engine.dataDog.incrementCounter(counterRedisAll, 1)
			r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
			return "", false
		}
		if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
			r.fillLogFields("[ORM][REDIS][RPOP]", start, "rpop", 1, 1,
				map[string]interface{}{"Key": key}, err)
		}
		r.engine.dataDog.incrementCounter(counterRedisAll, 1)
		r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
		if err != nil {
			panic(err)
		}
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

func (r *RedisCache) LSet(key string, index int64, value interface{}) {
	start := time.Now()
	_, err := r.client.LSet(key, index, value)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][LSET]", start, "lset", -1, 1,
			map[string]interface{}{"Key": key, "index": index, "value": value}, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysSet, 1)
	if err != nil {
		panic(err)
	}
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
	if err != nil {
		panic(err)
	}
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
	if err != nil {
		panic(err)
	}
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
	if err != nil {
		panic(err)
	}
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
	if err != nil {
		panic(err)
	}
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
	if err != nil {
		panic(err)
	}
	return val
}

func (r *RedisCache) SPop(key string) (string, bool) {
	start := time.Now()
	val, err := r.client.SPop(key)
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		errToReport := err
		if err == redis.Nil {
			errToReport = nil
		}
		r.fillLogFields("[ORM][REDIS][SPOP]", start, "spop", -1, 1,
			map[string]interface{}{"Key": key}, errToReport)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	if err != nil {
		if err == redis.Nil {
			return "", false
		}
		panic(err)
	}
	return val, true
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
	if err != nil {
		panic(err)
	}
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
	if err != nil {
		panic(err)
	}
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
	if err != nil {
		panic(err)
	}
	return val
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
	if err != nil {
		panic(err)
	}
	return val
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
	if err != nil {
		panic(err)
	}
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
	if err != nil {
		panic(err)
	}
}

func (r *RedisCache) MGet(keys ...string) map[string]interface{} {
	start := time.Now()
	val, err := r.client.MGet(keys...)
	if err != nil {
		if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
			r.fillLogFields("[ORM][REDIS][MGET]", start, "mget", 0, len(keys),
				map[string]interface{}{"Keys": keys}, err)
		}
		r.engine.dataDog.incrementCounter(counterRedisAll, 1)
		r.engine.dataDog.incrementCounter(counterRedisKeysGet, uint(len(keys)))
		panic(err)
	}
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
			map[string]interface{}{"Keys": keys}, nil)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, uint(len(keys)))
	return results
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
	if err != nil {
		panic(err)
	}
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
	if err != nil {
		panic(err)
	}
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
	if err != nil {
		panic(err)
	}
}

func (r *RedisCache) FlushDB() {
	start := time.Now()
	err := r.client.FlushDB()
	if r.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][FLUSHDB]", start, "flushdb", -1, 1, nil, err)
	}
	r.engine.dataDog.incrementCounter(counterRedisAll, 1)
	r.engine.dataDog.incrementCounter(counterRedisKeysGet, 1)
	if err != nil {
		panic(err)
	}
}

func (r *RedisCache) fillLogFields(message string, start time.Time, operation string, misses int, keys int, fields map[string]interface{}, err error) {
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
