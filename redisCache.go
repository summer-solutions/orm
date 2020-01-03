package orm

import (
	"encoding/json"
	"fmt"
	"github.com/bsm/redislock"
	"github.com/go-redis/redis"
	"strings"
	"time"
)

type RedisCache struct {
	code    string
	client  *redis.Client
	loggers []CacheLogger
}

type GetSetProvider func() interface{}

func (r *RedisCache) GetLock(key string, seconds int) (*redislock.Lock, error) {

	locker := redislock.New(r.client)
	lock, err := locker.Obtain(key, time.Duration(seconds)*time.Second, nil)
	if err == redislock.ErrNotObtained {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	return lock, nil
}

func (r *RedisCache) GetSet(key string, ttlSeconds int, provider GetSetProvider) (interface{}, error) {
	val, has, err := r.Get(key)
	if err != nil {
		return nil, err
	}
	if !has {
		userVal := provider()
		encoded, err := json.Marshal(userVal)
		if err != nil {
			return nil, err
		}
		err = r.Set(key, string(encoded), ttlSeconds)
		if err != nil {
			return nil, err
		}
		return userVal, nil
	}
	var data interface{}
	err = json.Unmarshal([]byte(val), &data)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (r *RedisCache) Get(key string) (value string, has bool, err error) {
	start := time.Now()
	val, err := r.client.Get(key).Result()
	if err != nil {
		if err == redis.Nil {
			r.log(key, "GET", time.Now().Sub(start).Microseconds(), 1)
			return "", false, nil
		}
		return "", false, err
	}
	r.log(key, "GET", time.Now().Sub(start).Microseconds(), 0)
	return val, true, nil
}

func (r *RedisCache) LRange(key string, start, stop int64) ([]string, error) {
	s := time.Now()
	val, err := r.client.LRange(key, start, stop).Result()
	if err != nil {
		return nil, err
	}
	r.log(key, fmt.Sprintf("LRANGE %d %d", start, stop), time.Now().Sub(s).Microseconds(), 0)
	return val, nil
}

func (r *RedisCache) HMget(key string, fields ...string) (map[string]interface{}, error) {
	start := time.Now()
	val, err := r.client.HMGet(key, fields...).Result()
	if err != nil {
		return nil, err
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
	return results, nil
}

func (r *RedisCache) HGetAll(key string) (map[string]string, error) {
	start := time.Now()
	val, err := r.client.HGetAll(key).Result()
	if err != nil {
		return nil, err
	}
	if r.loggers != nil {
		r.log(key, "HGETALL", time.Now().Sub(start).Microseconds(), 0)
	}
	return val, nil
}

func (r *RedisCache) LPush(key string, values ...interface{}) (int64, error) {
	start := time.Now()
	val, err := r.client.LPush(key, values...).Result()
	if err != nil {
		return 0, err
	}
	if r.loggers != nil {
		r.log(key, fmt.Sprintf("LPUSH %d values", len(values)), time.Now().Sub(start).Microseconds(), 0)
	}
	return val, nil
}

func (r *RedisCache) RPush(key string, values ...interface{}) (int64, error) {
	start := time.Now()
	val, err := r.client.RPush(key, values...).Result()
	if err != nil {
		return 0, err
	}
	if r.loggers != nil {
		r.log(key, fmt.Sprintf("RPUSH %d values", len(values)), time.Now().Sub(start).Microseconds(), 0)
	}
	return val, nil
}

func (r *RedisCache) RPop(key string) (value string, found bool, err error) {
	start := time.Now()
	val, err := r.client.RPop(key).Result()
	if err != nil {
		if err == redis.Nil {
			r.log(key, "RPOP", time.Now().Sub(start).Microseconds(), 1)
			return "", false, nil
		}
		r.log(key, "RPOP", time.Now().Sub(start).Microseconds(), 0)
		return "", false, err
	}
	r.log(key, "RPOP", time.Now().Sub(start).Microseconds(), 0)
	return val, true, nil
}

func (r *RedisCache) LSet(key string, index int64, value interface{}) error {
	start := time.Now()
	_, err := r.client.LSet(key, index, value).Result()
	if err != nil {
		return err
	}
	if r.loggers != nil {
		r.log(key, fmt.Sprintf("LSET %d %s", index, value), time.Now().Sub(start).Microseconds(), 0)
	}
	return nil
}

func (r *RedisCache) ZCard(key string) (int64, error) {
	start := time.Now()
	val, err := r.client.ZCard(key).Result()
	r.log(key, "ZCARD", time.Now().Sub(start).Microseconds(), 0)
	if err != nil {
		return 0, err
	}
	return val, nil
}

func (r *RedisCache) ZPopMin(key string, count ...int64) ([]redis.Z, error) {
	start := time.Now()
	val, err := r.client.ZPopMin(key, count...).Result()
	if r.loggers != nil {
		r.log(key, fmt.Sprintf("ZPOPMIN %v", count), time.Now().Sub(start).Microseconds(), 0)
	}
	if err != nil {
		return nil, err
	}
	return val, nil
}

func (r *RedisCache) LLen(key string) (int64, error) {
	start := time.Now()
	val, err := r.client.LLen(key).Result()
	r.log(key, "LLEN", time.Now().Sub(start).Microseconds(), 0)
	if err != nil {
		return 0, err
	}
	return val, nil
}

func (r *RedisCache) ZAdd(key string, members ...redis.Z) (int64, error) {
	start := time.Now()
	val, err := r.client.ZAdd(key, members...).Result()
	if err != nil {
		return 0, err
	}
	if r.loggers != nil {
		r.log(key, fmt.Sprintf("ZADD %d values", len(members)), time.Now().Sub(start).Microseconds(), 0)
	}
	return val, nil
}

func (r *RedisCache) HMset(key string, fields map[string]interface{}) error {
	start := time.Now()
	_, err := r.client.HMSet(key, fields).Result()
	if err != nil {
		return err
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
	return nil
}

func (r *RedisCache) HSet(key string, field string, value interface{}) error {
	start := time.Now()
	_, err := r.client.HSet(key, field, value).Result()
	if err != nil {
		return err
	}
	if r.loggers != nil {
		r.log(key, fmt.Sprintf("HSET %s $s", key, value), time.Now().Sub(start).Microseconds(), 0)
	}
	return nil
}

func (r *RedisCache) MGet(keys ...string) (map[string]interface{}, error) {
	start := time.Now()
	val, err := r.client.MGet(keys...).Result()
	if err != nil {
		return nil, err
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
	return results, nil
}

func (r *RedisCache) Set(key string, value interface{}, ttlSeconds int) error {
	start := time.Now()
	err := r.client.Set(key, value, time.Duration(ttlSeconds)*time.Second).Err()
	if err != nil {
		return err
	}
	r.log(key, fmt.Sprintf("SET [%ds]", ttlSeconds), time.Now().Sub(start).Microseconds(), 0)
	return nil
}

func (r *RedisCache) MSet(pairs ...interface{}) error {
	start := time.Now()
	err := r.client.MSet(pairs...).Err()
	if err != nil {
		return err
	}
	if r.loggers != nil {
		max := len(pairs)
		keys := make([]string, max)
		for i := 0; i < max; i += 2 {
			keys[i] = pairs[i].(string)
		}
		r.log("", fmt.Sprintf("MSET %v", keys), time.Now().Sub(start).Microseconds(), 0)
	}
	return nil
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

func (r *RedisCache) FlushDB() error {
	start := time.Now()
	err := r.client.FlushDB().Err()
	if err != nil {
		return err
	}
	r.log("", "FLUSHDB", time.Now().Sub(start).Microseconds(), 0)
	return nil
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
