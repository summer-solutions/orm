package orm

import (
	"fmt"
	"github.com/golang/groupcache/lru"
)

type LocalCache struct {
	code    string
	lru     *lru.Cache
	loggers []CacheLogger
	ttl     int64
	created int64
}

func (c *LocalCache) Get(key string) (value interface{}, ok bool) {
	value, ok = c.lru.Get(key)
	misses := 0
	if !ok {
		misses = 1
	}
	c.log(key, "GET", misses)
	return
}

func (c *LocalCache) MGet(keys ...string) []interface{} {
	results := make([]interface{}, len(keys))
	misses := 0
	for index, key := range keys {
		value, ok := c.lru.Get(key)
		if !ok {
			misses++
			value = nil
		}
		results[index] = value
	}
	c.log(fmt.Sprintf("%v", keys), "MGET", misses)
	return results
}

func (c *LocalCache) Set(key string, value interface{}) {
	c.lru.Add(key, value)
	c.log(key, "ADD", 0)
}

func (c *LocalCache) MSet(pairs ...interface{}) {
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
		c.log(fmt.Sprintf("%v", keys), "MSET", 0)
	}
}

func (c *LocalCache) Remove(key string) {
	c.lru.Remove(key)
	c.log(key, "REMOVE", 0)
}

func (c *LocalCache) RemoveMany(keys ...string) {
	for _, v := range keys {
		c.lru.Remove(v)
	}
	c.log("", fmt.Sprintf("REMOVE MANY %v", keys), 0)
}

func (c *LocalCache) Clear() {
	c.lru.Clear()
	c.log("", "CLEAR", 0)
}

func (c *LocalCache) AddLogger(logger CacheLogger) {
	if c.loggers == nil {
		c.loggers = make([]CacheLogger, 0)
	}
	c.loggers = append(c.loggers, logger)
}

func (c *LocalCache) log(key string, operation string, misses int) {
	if c.loggers != nil {
		for _, logger := range c.loggers {
			logger.Log("LOCAL", c.code, key, operation, misses)
		}
	}
}
