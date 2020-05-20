package orm

import (
	"encoding/json"
	"os"
	"time"

	"github.com/juju/errors"

	"github.com/apex/log"
	"github.com/apex/log/handlers/multi"
	"github.com/apex/log/handlers/text"
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
	engine     *Engine
	code       string
	client     redisClient
	log        *log.Entry
	logHandler *multi.Handler
}

type GetSetProvider func() interface{}

func (r *RedisCache) AddLogger(handler log.Handler) {
	r.logHandler.Handlers = append(r.logHandler.Handlers, handler)
}

func (r *RedisCache) SetLogLevel(level log.Level) {
	logger := log.Logger{Handler: r.logHandler, Level: level}
	r.log = logger.WithField("source", "orm")
	r.log.Level = level
}

func (r *RedisCache) EnableDebug() {
	r.AddLogger(text.New(os.Stdout))
	r.SetLogLevel(log.DebugLevel)
}

func (r *RedisCache) GetSet(key string, ttlSeconds int, provider GetSetProvider) (interface{}, error) {
	val, has, err := r.Get(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if !has {
		userVal := provider()
		encoded, _ := json.Marshal(userVal)
		err := r.Set(key, string(encoded), ttlSeconds)
		if err != nil {
			return nil, errors.Trace(err)
		}
		return userVal, nil
	}
	var data interface{}
	_ = json.Unmarshal([]byte(val), &data)
	return data, nil
}

func (r *RedisCache) Get(key string) (value string, has bool, err error) {
	start := time.Now()
	val, err := r.client.Get(key)
	if err != nil {
		if err == redis.Nil {
			if r.log != nil {
				r.fillLogFields(start, "get", 1).WithField("Key", key).Info("[ORM][REDIS][GET]")
			}
			return "", false, nil
		}
		return "", false, errors.Trace(err)
	}
	if r.log != nil {
		r.fillLogFields(start, "get", 0).WithField("Key", key).Info("[ORM][REDIS][GET]")
	}
	return val, true, nil
}

func (r *RedisCache) LRange(key string, start, stop int64) ([]string, error) {
	s := time.Now()
	val, err := r.client.LRange(key, start, stop)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if r.log != nil {
		r.fillLogFields(s, "lrange", -1).WithField("Key", key).
			WithField("start", start).WithField("stop", stop).Info("[ORM][REDIS][LRANGE]")
	}
	return val, nil
}

func (r *RedisCache) HMget(key string, fields ...string) (map[string]interface{}, error) {
	start := time.Now()
	val, err := r.client.HMGet(key, fields...)
	if err != nil {
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
	if r.log != nil {
		r.fillLogFields(start, "hmget", misses).WithField("Key", key).
			WithField("fields", fields).Info("[ORM][REDIS][HMGET]")
	}
	return results, nil
}

func (r *RedisCache) HGetAll(key string) (map[string]string, error) {
	start := time.Now()
	val, err := r.client.HGetAll(key)
	if err != nil {
		return nil, errors.Trace(err)
	}
	if r.log != nil {
		r.fillLogFields(start, "hgetall", -1).WithField("Key", key).Info("[ORM][REDIS][HGETALL]")
	}
	return val, nil
}

func (r *RedisCache) LPush(key string, values ...interface{}) (int64, error) {
	start := time.Now()
	val, err := r.client.LPush(key, values...)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if r.log != nil {
		r.fillLogFields(start, "lpush", -1).
			WithField("Key", key).WithField("values", values).Info("[ORM][REDIS][LPUSH]")
	}
	return val, nil
}

func (r *RedisCache) RPush(key string, values ...interface{}) (int64, error) {
	start := time.Now()
	val, err := r.client.RPush(key, values...)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if r.log != nil {
		r.fillLogFields(start, "rpush", -1).
			WithField("Key", key).WithField("values", values).Info("[ORM][REDIS][RPUSH]")
	}
	return val, nil
}

func (r *RedisCache) RPop(key string) (value string, found bool, err error) {
	start := time.Now()
	val, err := r.client.RPop(key)
	if err != nil {
		if err == redis.Nil {
			if r.log != nil {
				r.fillLogFields(start, "rpop", 1).WithField("Key", key).Info("[ORM][REDIS][RPOP]")
			}
			return "", false, nil
		}
		return "", false, errors.Trace(err)
	}
	if r.log != nil {
		r.fillLogFields(start, "rpop", 0).WithField("Key", key).Info("[ORM][REDIS][RPOP]")
	}
	return val, true, nil
}

func (r *RedisCache) LSet(key string, index int64, value interface{}) error {
	start := time.Now()
	_, err := r.client.LSet(key, index, value)
	if err != nil {
		return errors.Trace(err)
	}
	if r.log != nil {
		r.fillLogFields(start, "lset", -1).
			WithField("Key", key).WithField("index", index).WithField("value", value).Info("[ORM][REDIS][LSET]")
	}
	return nil
}

func (r *RedisCache) LRem(key string, count int64, value interface{}) error {
	start := time.Now()
	_, err := r.client.LRem(key, count, value)
	if err != nil {
		return errors.Trace(err)
	}
	if r.log != nil {
		r.fillLogFields(start, "lrem", -1).
			WithField("Key", key).WithField("count", count).WithField("value", value).Info("[ORM][REDIS][LREM]")
	}
	return nil
}

func (r *RedisCache) Ltrim(key string, start, stop int64) error {
	s := time.Now()
	_, err := r.client.LTrim(key, start, stop)
	if err != nil {
		return errors.Trace(err)
	}
	if r.log != nil {
		r.fillLogFields(s, "ltrim", -1).
			WithField("Key", key).WithField("start", start).WithField("stop", stop).Info("[ORM][REDIS][LTRIM]")
	}
	return nil
}

func (r *RedisCache) ZCard(key string) (int64, error) {
	start := time.Now()
	val, err := r.client.ZCard(key)
	if r.log != nil {
		r.fillLogFields(start, "zcard", -1).
			WithField("Key", key).Info("[ORM][REDIS][ZCARD]")
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	return val, nil
}

func (r *RedisCache) SCard(key string) (int64, error) {
	start := time.Now()
	val, err := r.client.SCard(key)
	if r.log != nil {
		r.fillLogFields(start, "scard", -1).
			WithField("Key", key).Info("[ORM][REDIS][SCARD]")
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	return val, nil
}

func (r *RedisCache) ZCount(key string, min, max string) (int64, error) {
	start := time.Now()
	val, err := r.client.ZCount(key, min, max)
	if r.log != nil {
		r.fillLogFields(start, "zcount", -1).
			WithField("Key", key).WithField("min", min).WithField("max", max).Info("[ORM][REDIS][ZCOUNT]")
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	return val, nil
}

func (r *RedisCache) SPop(key string) (string, bool, error) {
	start := time.Now()
	val, err := r.client.SPop(key)
	if r.log != nil {
		r.fillLogFields(start, "spop", -1).
			WithField("Key", key).Info("[ORM][REDIS][SPOP]")
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
	if r.log != nil {
		r.fillLogFields(start, "spopn", -1).
			WithField("Key", key).WithField("max", max).Info("[ORM][REDIS][SPOPN]")
	}
	if err != nil {
		return nil, errors.Trace(err)
	}
	return val, nil
}

func (r *RedisCache) LLen(key string) (int64, error) {
	start := time.Now()
	val, err := r.client.LLen(key)
	if r.log != nil {
		r.fillLogFields(start, "llen", -1).
			WithField("Key", key).Info("[ORM][REDIS][LLEN]")
	}
	if err != nil {
		return 0, errors.Trace(err)
	}
	return val, nil
}

func (r *RedisCache) ZAdd(key string, members ...*redis.Z) (int64, error) {
	start := time.Now()
	val, err := r.client.ZAdd(key, members...)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if r.log != nil {
		r.fillLogFields(start, "zadd", -1).
			WithField("Key", key).WithField("members", len(members)).Info("[ORM][REDIS][ZADD]")
	}
	return val, nil
}

func (r *RedisCache) SAdd(key string, members ...interface{}) (int64, error) {
	start := time.Now()
	val, err := r.client.SAdd(key, members...)
	if err != nil {
		return 0, errors.Trace(err)
	}
	if r.log != nil {
		r.fillLogFields(start, "sadd", -1).
			WithField("Key", key).WithField("members", len(members)).Info("[ORM][REDIS][SADD]")
	}
	return val, nil
}

func (r *RedisCache) HMset(key string, fields map[string]interface{}) error {
	start := time.Now()
	_, err := r.client.HMSet(key, fields)
	if err != nil {
		return errors.Trace(err)
	}
	if r.log != nil {
		r.fillLogFields(start, "hmset", -1).
			WithField("Key", key).WithField("fields", fields).Info("[ORM][REDIS][HMSET]")
	}
	return nil
}

func (r *RedisCache) HSet(key string, field string, value interface{}) error {
	start := time.Now()
	_, err := r.client.HSet(key, field, value)
	if err != nil {
		return errors.Trace(err)
	}
	if r.log != nil {
		r.fillLogFields(start, "hset", -1).
			WithField("Key", key).WithField("field", field).WithField("value", value).Info("[ORM][REDIS][HSET]")
	}
	return nil
}

func (r *RedisCache) MGet(keys ...string) (map[string]interface{}, error) {
	start := time.Now()
	val, err := r.client.MGet(keys...)
	if err != nil {
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
	if r.log != nil {
		r.fillLogFields(start, "mget", misses).
			WithField("Keys", keys).Info("[ORM][REDIS][MGET]")
	}
	return results, nil
}

func (r *RedisCache) Set(key string, value interface{}, ttlSeconds int) error {
	start := time.Now()
	err := r.client.Set(key, value, time.Duration(ttlSeconds)*time.Second)
	if err != nil {
		return errors.Trace(err)
	}
	if r.log != nil {
		r.fillLogFields(start, "set", -1).
			WithField("Key", key).WithField("value", value).WithField("ttl", ttlSeconds).Info("[ORM][REDIS][SET]")
	}
	return nil
}

func (r *RedisCache) MSet(pairs ...interface{}) error {
	start := time.Now()
	err := r.client.MSet(pairs...)
	if err != nil {
		return errors.Trace(err)
	}
	if r.log != nil {
		r.fillLogFields(start, "mset", -1).
			WithField("Pairs", pairs).Info("[ORM][REDIS][MSET]")
	}
	return nil
}

func (r *RedisCache) Del(keys ...string) error {
	start := time.Now()
	err := r.client.Del(keys...)
	if err != nil {
		return errors.Trace(err)
	}
	if r.log != nil {
		r.fillLogFields(start, "del", -1).
			WithField("Keys", keys).Info("[ORM][REDIS][DEL]")
	}
	return nil
}

func (r *RedisCache) FlushDB() error {
	start := time.Now()
	err := r.client.FlushDB()
	if err != nil {
		return errors.Trace(err)
	}
	if r.log != nil {
		r.fillLogFields(start, "flushdb", -1).Info("[ORM][REDIS][FLUSHDB]")
	}
	return nil
}

func (r *RedisCache) fillLogFields(start time.Time, operation string, misses int) *log.Entry {
	e := r.log.
		WithField("microseconds", time.Since(start).Microseconds()).
		WithField("operation", operation).
		WithField("pool", r.code).
		WithField("target", "redis").
		WithField("time", start.Unix())
	if misses >= 0 {
		e = e.WithField("misses", misses)
	}
	return e
}
