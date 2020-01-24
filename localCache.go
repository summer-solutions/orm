package orm

import (
	"container/list"
	"fmt"
	"github.com/golang/groupcache/lru"
	"time"
)

type LocalCache struct {
	code    string
	lru     *lru.Cache
	loggers *list.List
	ttl     int64
	created int64
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
			return ttlVal
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
	c.log(key, "GET", time.Now().Sub(start).Microseconds(), misses)
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
	c.log(fmt.Sprintf("%v", keys), "MGET", time.Now().Sub(start).Microseconds(), misses)
	return results
}

func (c *LocalCache) Set(key string, value interface{}) {
	start := time.Now()
	c.lru.Add(key, value)
	c.log(key, "ADD", time.Now().Sub(start).Microseconds(), 0)
}

func (c *LocalCache) MSet(pairs ...interface{}) {
	start := time.Now()
	max := len(pairs)
	for i := 0; i < max; i += 2 {
		c.lru.Add(pairs[i], pairs[i+1])
	}

	if c.loggers != nil {
		keys := make([]string, max/2)
		j := 0
		for i := 0; i < max; i += 2 {
			keys[j] = pairs[i].(string)
			j++
		}
		c.log(fmt.Sprintf("%v", keys), "MSET", time.Now().Sub(start).Microseconds(), 0)
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
		c.log(key, fmt.Sprintf("HMGET %v", fields), time.Now().Sub(start).Microseconds(), misses)
	}
	return results
}

func (c *LocalCache) HMset(key string, fields map[string]interface{}) {
	start := time.Now()
	m, has := c.lru.Get(key)
	if !has {
		m = make(map[string]interface{})
		c.lru.Add(key, m)
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
		c.log(key, fmt.Sprintf("HMSET %v", keys), time.Now().Sub(start).Microseconds(), 0)
	}
}

func (c *LocalCache) Remove(keys ...string) {
	start := time.Now()
	for _, v := range keys {
		c.lru.Remove(v)
	}
	c.log("", fmt.Sprintf("REMOVE MANY %v", keys), time.Now().Sub(start).Microseconds(), 0)
}

func (c *LocalCache) Clear() {
	start := time.Now()
	c.lru.Clear()
	c.log("", "CLEAR", time.Now().Sub(start).Microseconds(), 0)
}

func (c *LocalCache) RegisterLogger(logger CacheLogger) *list.Element {
	if c.loggers == nil {
		c.loggers = list.New()
	}
	return c.loggers.PushFront(logger)
}

func (c *LocalCache) UnregisterLogger(element *list.Element) {
	c.loggers.Remove(element)
}

func (c *LocalCache) log(key string, operation string, microseconds int64, misses int) {
	if c.loggers != nil {
		for e := c.loggers.Front(); e != nil; e = e.Next() {
			e.Value.(CacheLogger)("LOCAL", c.code, key, operation, microseconds, misses)
		}
	}
}
