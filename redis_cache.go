package orm

import (
	"reflect"
	"runtime/debug"
	"strings"
	"time"

	"github.com/juju/errors"

	jsoniter "github.com/json-iterator/go"

	"github.com/go-redis/redis/v7"
)

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
}

func (c *standardRedisClient) Get(key string) (string, error) {
	return c.client.Get(key).Result()
}

func (c *standardRedisClient) LRange(key string, start, stop int64) ([]string, error) {
	return c.client.LRange(key, start, stop).Result()
}

func (c *standardRedisClient) HMGet(key string, fields ...string) ([]interface{}, error) {
	return c.client.HMGet(key, fields...).Result()
}

func (c *standardRedisClient) HGetAll(key string) (map[string]string, error) {
	return c.client.HGetAll(key).Result()
}

func (c *standardRedisClient) LPush(key string, values ...interface{}) (int64, error) {
	return c.client.LPush(key, values...).Result()
}

func (c *standardRedisClient) RPush(key string, values ...interface{}) (int64, error) {
	return c.client.RPush(key, values...).Result()
}

func (c *standardRedisClient) RPop(key string) (string, error) {
	return c.client.RPop(key).Result()
}

func (c *standardRedisClient) LSet(key string, index int64, value interface{}) (string, error) {
	return c.client.LSet(key, index, value).Result()
}

func (c *standardRedisClient) LRem(key string, count int64, value interface{}) (int64, error) {
	return c.client.LRem(key, count, value).Result()
}

func (c *standardRedisClient) LTrim(key string, start, stop int64) (string, error) {
	return c.client.LTrim(key, start, stop).Result()
}

func (c *standardRedisClient) ZCard(key string) (int64, error) {
	return c.client.ZCard(key).Result()
}

func (c *standardRedisClient) SCard(key string) (int64, error) {
	return c.client.SCard(key).Result()
}

func (c *standardRedisClient) ZCount(key string, min, max string) (int64, error) {
	return c.client.ZCount(key, min, max).Result()
}

func (c *standardRedisClient) SPop(key string) (string, error) {
	return c.client.SPop(key).Result()
}

func (c *standardRedisClient) SPopN(key string, max int64) ([]string, error) {
	return c.client.SPopN(key, max).Result()
}

func (c *standardRedisClient) LLen(key string) (int64, error) {
	return c.client.LLen(key).Result()
}

func (c *standardRedisClient) ZAdd(key string, members ...*redis.Z) (int64, error) {
	return c.client.ZAdd(key, members...).Result()
}

func (c *standardRedisClient) SAdd(key string, members ...interface{}) (int64, error) {
	return c.client.SAdd(key, members...).Result()
}

func (c *standardRedisClient) HMSet(key string, fields map[string]interface{}) (bool, error) {
	return c.client.HMSet(key, fields).Result()
}

func (c *standardRedisClient) HSet(key string, field string, value interface{}) (int64, error) {
	return c.client.HSet(key, field, value).Result()
}

func (c *standardRedisClient) MGet(keys ...string) ([]interface{}, error) {
	return c.client.MGet(keys...).Result()
}

func (c *standardRedisClient) Set(key string, value interface{}, expiration time.Duration) error {
	return c.client.Set(key, value, expiration).Err()
}

func (c *standardRedisClient) MSet(pairs ...interface{}) error {
	return c.client.MSet(pairs...).Err()
}

func (c *standardRedisClient) Del(keys ...string) error {
	return c.client.Del(keys...).Err()
}

func (c *standardRedisClient) FlushDB() error {
	return c.client.FlushDB().Err()
}

type RedisCache struct {
	engine *Engine
	code   string
	client redisClient
}

type GetSetProvider func() interface{}

func (r *RedisCache) GetSet(key string, ttlSeconds int, provider GetSetProvider) (interface{}, error) {
	val, has, err := r.Get(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !has {
		userVal := provider()
		encoded, _ := jsoniter.ConfigFastest.Marshal(userVal)
		err := r.Set(key, string(encoded), ttlSeconds)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return userVal, nil
	}
	var data interface{}
	_ = jsoniter.ConfigFastest.Unmarshal([]byte(val), &data)
	return data, nil
}

func (r *RedisCache) Get(key string) (value string, has bool, err error) {
	start := time.Now()
	val, err := r.client.Get(key)
	if err != nil {
		if err == redis.Nil {
			if r.engine.loggers[LoggerSourceRedis] != nil {
				r.fillLogFields("[ORM][REDIS][GET]", start, false, false,
					"get", 1, 1, map[string]interface{}{"Key": key}, nil)
			}
			return "", false, nil
		}
		if r.engine.loggers[LoggerSourceRedis] != nil {
			r.fillLogFields("[ORM][REDIS][GET]", start, false, false, "get", 1, 1, map[string]interface{}{"Key": key}, err)
		}
		return "", false, errors.Trace(err)
	}
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][GET]", start, false, false, "get", 0, 1, map[string]interface{}{"Key": key}, nil)
	}
	return val, true, nil
}

func (r *RedisCache) LRange(key string, start, stop int64) ([]string, error) {
	s := time.Now()
	val, err := r.client.LRange(key, start, stop)
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][LRANGE]", s, false, false, "lrange", -1, len(val),
			map[string]interface{}{"Key": key, "start": start, "stop": stop}, err)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return val, nil
}

func (r *RedisCache) HMget(key string, fields ...string) (map[string]interface{}, error) {
	start := time.Now()
	val, err := r.client.HMGet(key, fields...)
	if err != nil {
		if r.engine.loggers[LoggerSourceRedis] != nil {
			r.fillLogFields("[ORM][REDIS][HMGET]", start, false, false, "hmget", 0, len(fields),
				map[string]interface{}{"Key": key, "fields": fields}, err)
		}
		return nil, errors.Trace(err)
	}
	results := make(map[string]interface{}, len(fields))
	misses := 0
	for index, v := range val {
		if v == nil {
			misses++
		}
		results[fields[index]] = v
	}
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][HMGET]", start, false, false, "hmget", misses, len(fields),
			map[string]interface{}{"Key": key, "fields": fields}, nil)
	}
	return results, nil
}

func (r *RedisCache) HGetAll(key string) (map[string]string, error) {
	start := time.Now()
	val, err := r.client.HGetAll(key)
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][HGETALL]", start, false, false, "hgetall", -1, len(val),
			map[string]interface{}{"Key": key}, err)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return val, nil
}

func (r *RedisCache) LPush(key string, values ...interface{}) (int64, error) {
	start := time.Now()
	val, err := r.client.LPush(key, values...)
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][LPUSH]", start, true, false, "lpush", -1, len(values),
			map[string]interface{}{"Key": key, "values": values}, err)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	return val, nil
}

func (r *RedisCache) RPush(key string, values ...interface{}) (int64, error) {
	start := time.Now()
	val, err := r.client.RPush(key, values...)
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][RPUSH]", start, true, false, "rpush", -1, len(values),
			map[string]interface{}{"Key": key, "values": values}, err)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	return val, nil
}

func (r *RedisCache) RPop(key string) (value string, found bool, err error) {
	start := time.Now()
	val, err := r.client.RPop(key)
	if err != nil {
		if err == redis.Nil {
			if r.engine.loggers[LoggerSourceRedis] != nil {
				r.fillLogFields("[ORM][REDIS][RPOP]", start, false, true, "rpop", 1, 1,
					map[string]interface{}{"Key": key}, nil)
			}
			return "", false, nil
		}
		if r.engine.loggers[LoggerSourceRedis] != nil {
			r.fillLogFields("[ORM][REDIS][RPOP]", start, false, true, "rpop", 1, 1,
				map[string]interface{}{"Key": key}, err)
		}
		return "", false, errors.Trace(err)
	}
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][RPOP]", start, false, true, "rpop", 0, 1,
			map[string]interface{}{"Key": key}, nil)
	}
	return val, true, nil
}

func (r *RedisCache) LSet(key string, index int64, value interface{}) error {
	start := time.Now()
	_, err := r.client.LSet(key, index, value)
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][LSET]", start, true, false, "lset", -1, 1,
			map[string]interface{}{"Key": key, "index": index, "value": value}, err)
	}
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (r *RedisCache) LRem(key string, count int64, value interface{}) error {
	start := time.Now()
	_, err := r.client.LRem(key, count, value)
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][LREM]", start, false, true, "lrem", -1, 1,
			map[string]interface{}{"Key": key, "count": count, "value": value}, err)
	}
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (r *RedisCache) Ltrim(key string, start, stop int64) error {
	s := time.Now()
	_, err := r.client.LTrim(key, start, stop)
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][LTRIM]", s, false, true, "ltrim", -1, 1,
			map[string]interface{}{"Key": key, "start": start, "stop": stop}, err)
	}
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (r *RedisCache) ZCard(key string) (int64, error) {
	start := time.Now()
	val, err := r.client.ZCard(key)
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][ZCARD]", start, false, false, "zcard", -1, 1,
			map[string]interface{}{"Key": key}, err)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	return val, nil
}

func (r *RedisCache) SCard(key string) (int64, error) {
	start := time.Now()
	val, err := r.client.SCard(key)
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][SCARD]", start, false, false, "scard", -1, 1,
			map[string]interface{}{"Key": key}, err)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	return val, nil
}

func (r *RedisCache) ZCount(key string, min, max string) (int64, error) {
	start := time.Now()
	val, err := r.client.ZCount(key, min, max)
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][ZCOUNT]", start, false, false, "zcount", -1, 1,
			map[string]interface{}{"Key": key, "min": min, "max": max}, err)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	return val, nil
}

func (r *RedisCache) SPop(key string) (string, bool, error) {
	start := time.Now()
	val, err := r.client.SPop(key)
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][SPOP]", start, false, true, "spop", -1, 1,
			map[string]interface{}{"Key": key}, err)
	}
	if err != nil {
		if err == redis.Nil {
			return "", false, nil
		}
		return "", false, errors.Trace(err)
	}
	return val, true, nil
}

func (r *RedisCache) SPopN(key string, max int64) ([]string, error) {
	start := time.Now()
	val, err := r.client.SPopN(key, max)
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][SPOPN]", start, false, true, "spopn", -1, 1,
			map[string]interface{}{"Key": key, "max": max}, err)
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return val, nil
}

func (r *RedisCache) LLen(key string) (int64, error) {
	start := time.Now()
	val, err := r.client.LLen(key)
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][LLEN]", start, false, false, "llen", -1, 1,
			map[string]interface{}{"Key": key}, err)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	return val, nil
}

func (r *RedisCache) ZAdd(key string, members ...*redis.Z) (int64, error) {
	start := time.Now()
	val, err := r.client.ZAdd(key, members...)
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][ZADD]", start, true, false, "zadd", -1, len(members),
			map[string]interface{}{"Key": key, "members": len(members)}, err)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	return val, nil
}

func (r *RedisCache) SAdd(key string, members ...interface{}) (int64, error) {
	start := time.Now()
	val, err := r.client.SAdd(key, members...)
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][SADD]", start, true, false, "sadd", -1, len(members),
			map[string]interface{}{"Key": key, "members": len(members)}, err)
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	return val, nil
}

func (r *RedisCache) HMset(key string, fields map[string]interface{}) error {
	start := time.Now()
	_, err := r.client.HMSet(key, fields)
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][HMSET]", start, true, false, "hmset", -1, len(fields),
			map[string]interface{}{"Key": key, "fields": fields}, err)
	}
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (r *RedisCache) HSet(key string, field string, value interface{}) error {
	start := time.Now()
	_, err := r.client.HSet(key, field, value)
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][HSET]", start, true, false, "hset", -1, 1,
			map[string]interface{}{"Key": key, "field": field, "value": value}, err)
	}
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (r *RedisCache) MGet(keys ...string) (map[string]interface{}, error) {
	start := time.Now()
	val, err := r.client.MGet(keys...)
	if err != nil {
		if r.engine.loggers[LoggerSourceRedis] != nil {
			r.fillLogFields("[ORM][REDIS][MGET]", start, false, false, "mget", 0, len(keys),
				map[string]interface{}{"Keys": keys}, err)
		}
		return nil, errors.Trace(err)
	}
	results := make(map[string]interface{}, len(keys))
	misses := 0
	for index, v := range val {
		results[keys[index]] = v
		if v == nil {
			misses++
		}
	}
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][MGET]", start, false, false, "mget", misses, len(keys),
			map[string]interface{}{"Keys": keys}, nil)
	}
	return results, nil
}

func (r *RedisCache) Set(key string, value interface{}, ttlSeconds int) error {
	start := time.Now()
	err := r.client.Set(key, value, time.Duration(ttlSeconds)*time.Second)
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][SET]", start, true, false, "set", -1, 1,
			map[string]interface{}{"Key": key, "value": value, "ttl": ttlSeconds}, err)
	}
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (r *RedisCache) MSet(pairs ...interface{}) error {
	start := time.Now()
	err := r.client.MSet(pairs...)
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][MSET]", start, true, false, "mset", -1, len(pairs),
			map[string]interface{}{"Pairs": pairs}, err)
	}
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (r *RedisCache) Del(keys ...string) error {
	start := time.Now()
	err := r.client.Del(keys...)
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][DEL]", start, false, true, "del", -1, len(keys),
			map[string]interface{}{"Keys": keys}, err)
	}
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (r *RedisCache) FlushDB() error {
	start := time.Now()
	err := r.client.FlushDB()
	if r.engine.loggers[LoggerSourceRedis] != nil {
		r.fillLogFields("[ORM][REDIS][FLUSHDB]", start, false, true, "flushdb", -1, 1, nil, err)
	}
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}

func (r *RedisCache) fillLogFields(message string, start time.Time, isSet bool, isDelete bool,
	operation string, misses int, keys int, fields map[string]interface{}, err error) {
	now := time.Now()
	stop := time.Since(start).Microseconds()
	e := r.engine.loggers[LoggerSourceRedis].log.
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
	if isSet {
		e = e.WithField("is_set", 1)
	} else if isDelete {
		e = e.WithField("is_delete", 1)
	}
	if err != nil {
		stackParts := strings.Split(errors.ErrorStack(err), "\n")
		stack := strings.Join(stackParts[1:], "\\n")
		fullStack := strings.Join(strings.Split(string(debug.Stack()), "\n")[4:], "\\n")
		e.WithError(err).
			WithField("stack", stack).
			WithField("stack_full", fullStack).
			WithField("error_type", reflect.TypeOf(errors.Cause(err)).String()).
			Error(message)
	} else {
		e.Info(message)
	}
}
