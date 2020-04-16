package orm

import (
	"fmt"
	"sync"
	"time"

	"github.com/golang/groupcache/lru"
)

type LocalCache struct {
	engine  *Engine
	code    string
	lru     *lru.Cache
	loggers []CacheLogger
	ttl     int64
}

type ttlValue struct {
	value interface{}
	time  int64
}

func (c *LocalCache) GetSet(key string, ttlSeconds int, provider GetSetProvider) interface{} {
	val, has := c.Get(key)
	if has {
		ttlVal := val.(ttlValue)
		if time.Now().Unix()-ttlVal.time <= int64(ttlSeconds) {
			return ttlVal.value
		}
	}
	userVal := provider()
	val = ttlValue{value: userVal, time: time.Now().Unix()}
	c.Set(key, val)
	return userVal
}

func (c *LocalCache) Get(key string) (value interface{}, ok bool) {
	start := time.Now()
	value, ok = c.lru.Get(key)
	misses := 0
	if !ok {
		misses = 1
	}
	c.log(key, "GET", time.Since(start).Microseconds(), misses)
	return
}

func (c *LocalCache) MGet(keys ...string) map[string]interface{} {
	start := time.Now()
	results := make(map[string]interface{}, len(keys))
	misses := 0
	for _, key := range keys {
		value, ok := c.lru.Get(key)
		if !ok {
			misses++
			value = nil
		}
		results[key] = value
	}
	c.log(fmt.Sprintf("%v", keys), "MGET", time.Since(start).Microseconds(), misses)
	return results
}

func (c *LocalCache) Set(key string, value interface{}) {
	start := time.Now()
	m := &sync.Mutex{}
	m.Lock()
	c.lru.Add(key, value)
	m.Unlock()
	c.log(key, "ADD", time.Since(start).Microseconds(), 0)
}

func (c *LocalCache) MSet(pairs ...interface{}) {
	start := time.Now()
	max := len(pairs)
	m := &sync.Mutex{}
	m.Lock()
	for i := 0; i < max; i += 2 {
		c.lru.Add(pairs[i], pairs[i+1])
	}
	m.Unlock()

	if c.loggers != nil {
		keys := make([]string, max/2)
		j := 0
		for i := 0; i < max; i += 2 {
			keys[j] = pairs[i].(string)
			j++
		}
		c.log(fmt.Sprintf("%v", keys), "MSET", time.Since(start).Microseconds(), 0)
	}
}

func (c *LocalCache) HMget(key string, fields ...string) map[string]interface{} {
	start := time.Now()
	l := len(fields)
	results := make(map[string]interface{}, l)
	value, ok := c.lru.Get(key)
	misses := 0
	for _, field := range fields {
		if !ok {
			results[field] = nil
			misses++
		} else {
			val, has := value.(map[string]interface{})[field]
			if !has {
				results[field] = nil
				misses++
			} else {
				results[field] = val
			}
		}
	}
	if c.loggers != nil {
		c.log(key, fmt.Sprintf("HMGET %v", fields), time.Since(start).Microseconds(), misses)
	}
	return results
}

func (c *LocalCache) HMset(key string, fields map[string]interface{}) {
	start := time.Now()
	m, has := c.lru.Get(key)
	if !has {
		m = make(map[string]interface{})
		mutex := &sync.Mutex{}
		mutex.Lock()
		c.lru.Add(key, m)
		mutex.Unlock()
	}
	for k, v := range fields {
		m.(map[string]interface{})[k] = v
	}
	if c.loggers != nil {
		keys := make([]string, len(fields))
		i := 0
		for key := range fields {
			keys[i] = key
			i++
		}
		c.log(key, fmt.Sprintf("HMSET %v", keys), time.Since(start).Microseconds(), 0)
	}
}

func (c *LocalCache) Remove(keys ...string) {
	start := time.Now()
	m := &sync.Mutex{}
	m.Lock()
	for _, v := range keys {
		c.lru.Remove(v)
	}
	m.Unlock()
	c.log("", fmt.Sprintf("REMOVE MANY %v", keys), time.Since(start).Microseconds(), 0)
}

func (c *LocalCache) Clear() {
	start := time.Now()
	m := &sync.Mutex{}
	m.Lock()
	c.lru.Clear()
	m.Unlock()
	c.log("", "CLEAR", time.Since(start).Microseconds(), 0)
}

func (c *LocalCache) RegisterLogger(logger CacheLogger) {
	if c.loggers == nil {
		c.loggers = make([]CacheLogger, 0)
	}
	c.loggers = append(c.loggers, logger)
}

func (c *LocalCache) log(key string, operation string, microseconds int64, misses int) {
	if c.loggers != nil {
		for _, logger := range c.loggers {
			logger.Log("LOCAL", c.code, key, operation, microseconds, misses)
		}
	}
}
