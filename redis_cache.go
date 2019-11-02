package orm

import (
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

func (r *RedisCache) Get(key string) (value string, ok bool) {
	val, err := r.client.Get(key).Result()
	if err != nil {
		if err == redis.Nil {
			r.log(key, "GET", 1)
			return "", false
		}
		panic(err)
	}
	r.log(key, "GET", 0)
	return val, true
}

func (r *RedisCache) LRange(key string, start, stop int64) []string {
	val, err := r.client.LRange(key, start, stop).Result()
	if err != nil {
		panic(err)
	}
	r.log(key, fmt.Sprintf("LRANGE %d %d", start, stop), 0)
	return val
}

func (r *RedisCache) HMget(key string, fields ...string) []interface{} {
	val, err := r.client.HMGet(key, fields...).Result()
	if err != nil {
		panic(err)
	}
	if r.loggers != nil {
		misses := 0
		for _, v := range val {
			if v == nil {
				misses++
			}
		}
		r.log(key, fmt.Sprintf("HMGET %v", fields), misses)
	}
	return val
}

func (r *RedisCache) HMset(key string, fields map[string]interface{}) {
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
		r.log(key, fmt.Sprintf("HMSET %v", keys), 0)
	}
}

func (r *RedisCache) MGet(keys ...string) []interface{} {
	val, err := r.client.MGet(keys...).Result()
	if err != nil {
		panic(err)
	}
	if r.loggers != nil {
		misses := 0
		for _, v := range val {
			if v == nil {
				misses++
			}
		}
		r.log(strings.Join(keys, ","), "MGET", misses)
	}
	return val
}

func (r *RedisCache) Set(key string, value string, ttlSeconds int) {
	err := r.client.Set(key, value, time.Duration(ttlSeconds)*time.Second).Err()
	if err != nil {
		panic(err)
	}
	r.log(key, "SET", 0)
}

func (r *RedisCache) MSet(pairs ...interface{}) {
	err := r.client.MSet(pairs...).Err()
	if err != nil {
		panic(err)
	}
	if r.loggers != nil {
		max := len(pairs) / 2
		keys := make([]string, max)
		for i := 0; i < max; i += 2 {
			keys[i] = pairs[i].(string)
		}
		r.log("", fmt.Sprintf("MSET %v", keys), 0)
	}
}

func (r *RedisCache) Del(keys ...string) {
	err := r.client.Del(keys...).Err()
	if err != nil {
		panic(err)
	}
	r.log(strings.Join(keys, ","), "DELETE", 0)
}

func (r *RedisCache) FlushDB() {
	err := r.client.FlushDB().Err()
	if err != nil {
		panic(err)
	}
	r.log("", "FLUSHDB", 0)
}

func (r *RedisCache) AddLogger(logger CacheLogger) {
	if r.loggers == nil {
		r.loggers = make([]CacheLogger, 0)
	}
	r.loggers = append(r.loggers, logger)
}

func (r *RedisCache) log(key string, operation string, misses int) {
	if r.loggers != nil {
		for _, logger := range r.loggers {
			logger.Log("REDIS", r.code, key, operation, misses)
		}
	}
}
