package orm

import (
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v7"
	"strings"
	"time"
)

type RedisCache struct {
	code    string
	client  *redis.Client
	loggers []CacheLogger
}

type GetSetProvider func() interface{}

func (r *RedisCache) GetSet(key string, ttlSeconds int, provider GetSetProvider) (interface{}, error) {
	val, has := r.Get(key)
	if !has {
		userVal := provider()
		encoded, err := json.Marshal(userVal)
		if err != nil {
			return nil, err
		}
		r.Set(key, string(encoded), ttlSeconds)
		return userVal, nil
	}
	var data interface{}
	err := json.Unmarshal([]byte(val), &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (r *RedisCache) Get(key string) (value string, ok bool) {
	start := time.Now()
	val, err := r.client.Get(key).Result()
	if err != nil {
		if err == redis.Nil {
			r.log(key, "GET", time.Now().Sub(start).Microseconds(), 1)
			return "", false
		}
		panic(err)
	}
	r.log(key, "GET", time.Now().Sub(start).Microseconds(), 0)
	return val, true
}

func (r *RedisCache) LRange(key string, start, stop int64) []string {
	s := time.Now()
	val, err := r.client.LRange(key, start, stop).Result()
	if err != nil {
		panic(err)
	}
	r.log(key, fmt.Sprintf("LRANGE %d %d", start, stop), time.Now().Sub(s).Microseconds(), 0)
	return val
}

func (r *RedisCache) HMget(key string, fields ...string) map[string]interface{} {
	start := time.Now()
	val, err := r.client.HMGet(key, fields...).Result()
	if err != nil {
		panic(err)
	}
	results := make(map[string]interface{}, len(fields))
	misses := 0
	for index, v := range val {
		if v == nil {
			misses++
		}
		results[fields[index]] = v
	}
	if r.loggers != nil {
		r.log(key, fmt.Sprintf("HMGET %v", fields), time.Now().Sub(start).Microseconds(), misses)
	}
	return results
}

func (r *RedisCache) LPush(key string, values ...interface{}) int64 {
	start := time.Now()
	val, err := r.client.LPush(key, values...).Result()
	if err != nil {
		panic(err)
	}
	if r.loggers != nil {
		r.log(key, fmt.Sprintf("LPUSH %d values", len(values)), time.Now().Sub(start).Microseconds(), 0)
	}
	return val
}

func (r *RedisCache) RPush(key string, values ...interface{}) int64 {
	start := time.Now()
	val, err := r.client.RPush(key, values...).Result()
	if err != nil {
		panic(err)
	}
	if r.loggers != nil {
		r.log(key, fmt.Sprintf("RPUSH %d values", len(values)), time.Now().Sub(start).Microseconds(), 0)
	}
	return val
}

func (r *RedisCache) RPop(key string) (value string, found bool) {
	start := time.Now()
	val, err := r.client.RPop(key).Result()
	if err != nil {
		if err == redis.Nil {
			r.log(key, "RPOP", time.Now().Sub(start).Microseconds(), 1)
			return "", false
		}
		r.log(key, "RPOP", time.Now().Sub(start).Microseconds(), 0)
		panic(err)
	}
	r.log(key, "RPOP", time.Now().Sub(start).Microseconds(), 0)
	return val, true
}

func (r *RedisCache) ZCard(key string) int64 {
	start := time.Now()
	val, err := r.client.ZCard(key).Result()
	r.log(key, "ZCARD", time.Now().Sub(start).Microseconds(), 0)
	if err != nil {
		panic(err)
	}
	return val
}

func (r *RedisCache) ZPopMin(key string, count ...int64) []redis.Z {
	start := time.Now()
	val, err := r.client.ZPopMin(key, count...).Result()
	if r.loggers != nil {
		r.log(key, fmt.Sprintf("ZPOPMIN %v", count), time.Now().Sub(start).Microseconds(), 0)
	}
	if err != nil {
		panic(err)
	}
	return val
}

func (r *RedisCache) LLen(key string) int64 {
	start := time.Now()
	val, err := r.client.LLen(key).Result()
	r.log(key, "LLEN", time.Now().Sub(start).Microseconds(), 0)
	if err != nil {
		panic(err)
	}
	return val
}

func (r *RedisCache) ZAdd(key string, members ...*redis.Z) int64 {
	start := time.Now()
	val, err := r.client.ZAdd(key, members...).Result()
	if err != nil {
		panic(err)
	}
	if r.loggers != nil {
		r.log(key, fmt.Sprintf("ZADD %d values", len(members)), time.Now().Sub(start).Microseconds(), 0)
	}
	return val
}

func (r *RedisCache) HMset(key string, fields map[string]interface{}) {
	start := time.Now()
	_, err := r.client.HMSet(key, fields).Result()
	if err != nil {
		panic(err)
	}
	if r.loggers != nil {
		keys := make([]string, len(fields))
		i := 0
		for key := range fields {
			keys[i] = key
			i++
		}
		r.log(key, fmt.Sprintf("HMSET %v", keys), time.Now().Sub(start).Microseconds(), 0)
	}
}

func (r *RedisCache) MGet(keys ...string) map[string]interface{} {
	start := time.Now()
	val, err := r.client.MGet(keys...).Result()
	if err != nil {
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
	if r.loggers != nil {
		r.log(strings.Join(keys, ","), "MGET", time.Now().Sub(start).Microseconds(), misses)
	}
	return results
}

func (r *RedisCache) Set(key string, value string, ttlSeconds int) {
	start := time.Now()
	err := r.client.Set(key, value, time.Duration(ttlSeconds)*time.Second).Err()
	if err != nil {
		panic(err)
	}
	r.log(key, fmt.Sprintf("SET [%ds]", ttlSeconds), time.Now().Sub(start).Microseconds(), 0)
}

func (r *RedisCache) MSet(pairs ...interface{}) {
	start := time.Now()
	err := r.client.MSet(pairs...).Err()
	if err != nil {
		panic(err)
	}
	if r.loggers != nil {
		max := len(pairs)
		keys := make([]string, max)
		for i := 0; i < max; i += 2 {
			keys[i] = pairs[i].(string)
		}
		r.log("", fmt.Sprintf("MSET %v", keys), time.Now().Sub(start).Microseconds(), 0)
	}
}

func (r *RedisCache) Del(keys ...string) error {
	start := time.Now()
	err := r.client.Del(keys...).Err()
	if err != nil {
		return err
	}
	r.log(strings.Join(keys, ","), "DELETE", time.Now().Sub(start).Microseconds(), 0)
	return nil
}

func (r *RedisCache) FlushDB() {
	start := time.Now()
	err := r.client.FlushDB().Err()
	if err != nil {
		panic(err)
	}
	r.log("", "FLUSHDB", time.Now().Sub(start).Microseconds(), 0)
}

func (r *RedisCache) AddLogger(logger CacheLogger) {
	if r.loggers == nil {
		r.loggers = make([]CacheLogger, 0)
	}
	r.loggers = append(r.loggers, logger)
}

func (r *RedisCache) log(key string, operation string, microseconds int64, misses int) {
	if r.loggers != nil {
		for _, logger := range r.loggers {
			logger.Log("REDIS", r.code, key, operation, microseconds, misses)
		}
	}
}
