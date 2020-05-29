package orm

import (
	"sync"
	"time"

	"github.com/golang/groupcache/lru"
)

type LocalCache struct {
	engine *Engine
	code   string
	lru    *lru.Cache
	ttl    int64
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
	if c.engine.loggers[QueryLoggerSourceLocalCache] != nil {
		c.fillLogFields("[ORM][LOCAL][GET]", start, "get", misses, map[string]interface{}{"Key": key})
	}
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
	if c.engine.loggers[QueryLoggerSourceLocalCache] != nil {
		c.fillLogFields("[ORM][LOCAL][MGET]", start, "mget", misses, map[string]interface{}{"Keys": keys})
	}
	return results
}

func (c *LocalCache) Set(key string, value interface{}) {
	start := time.Now()
	m := &sync.Mutex{}
	m.Lock()
	defer m.Unlock()
	c.lru.Add(key, value)
	if c.engine.loggers[QueryLoggerSourceLocalCache] != nil {
		c.fillLogFields("[ORM][LOCAL][MGET]", start, "set", -1, map[string]interface{}{"Key": key, "value": value})
	}
}

func (c *LocalCache) MSet(pairs ...interface{}) {
	start := time.Now()
	max := len(pairs)
	m := &sync.Mutex{}
	m.Lock()
	defer m.Unlock()
	for i := 0; i < max; i += 2 {
		c.lru.Add(pairs[i], pairs[i+1])
	}
	if c.engine.loggers[QueryLoggerSourceLocalCache] != nil {
		c.fillLogFields("[ORM][LOCAL][MSET]", start, "mset", -1, map[string]interface{}{"Keys": pairs})
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
	if c.engine.loggers[QueryLoggerSourceLocalCache] != nil {
		c.fillLogFields("[ORM][LOCAL][HMGET]", start, "hmget", misses, map[string]interface{}{"Key": key, "fields": fields})
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
		defer mutex.Unlock()
		c.lru.Add(key, m)
	}
	for k, v := range fields {
		m.(map[string]interface{})[k] = v
	}
	if c.engine.loggers[QueryLoggerSourceLocalCache] != nil {
		c.fillLogFields("[ORM][LOCAL][HMSET]", start, "hmset", -1, map[string]interface{}{"Key": key, "fields": fields})
	}
}

func (c *LocalCache) Remove(keys ...string) {
	start := time.Now()
	m := &sync.Mutex{}
	m.Lock()
	defer m.Unlock()
	for _, v := range keys {
		c.lru.Remove(v)
	}
	if c.engine.loggers[QueryLoggerSourceLocalCache] != nil {
		c.fillLogFields("[ORM][LOCAL][REMOVE]", start, "remove", -1, map[string]interface{}{"Keys": keys})
	}
}

func (c *LocalCache) Clear() {
	start := time.Now()
	m := &sync.Mutex{}
	m.Lock()
	defer m.Unlock()
	c.lru.Clear()
	if c.engine.loggers[QueryLoggerSourceLocalCache] != nil {
		c.fillLogFields("[ORM][LOCAL][CLEAR]", start, "clear", -1, nil)
	}
}

func (c *LocalCache) fillLogFields(message string, start time.Time, operation string, misses int, fields map[string]interface{}) {
	stop := time.Since(start).Microseconds()
	e := c.engine.loggers[QueryLoggerSourceLocalCache].log.
		WithField("microseconds", stop).
		WithField("operation", operation).
		WithField("pool", c.code).
		WithField("target", "local_cache").
		WithField("time", start.Unix())
	if misses >= 0 {
		e = e.WithField("misses", misses)
	}
	for k, v := range fields {
		e = e.WithField(k, v)
	}
	e.Info(message)
}
