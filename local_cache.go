package orm

import (
	"os"
	"sync"
	"time"

	"github.com/apex/log"
	"github.com/apex/log/handlers/multi"
	"github.com/apex/log/handlers/text"

	"github.com/golang/groupcache/lru"
)

type LocalCache struct {
	engine     *Engine
	code       string
	lru        *lru.Cache
	ttl        int64
	log        *log.Entry
	logHandler *multi.Handler
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
	if c.log != nil {
		c.fillLogFields(start, "get", misses).WithField("Key", key).Info("[ORM][LOCAL][GET]")
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
	if c.log != nil {
		c.fillLogFields(start, "mget", misses).WithField("Keys", keys).Info("[ORM][LOCAL][MGET]")
	}
	return results
}

func (c *LocalCache) Set(key string, value interface{}) {
	start := time.Now()
	m := &sync.Mutex{}
	m.Lock()
	c.lru.Add(key, value)
	m.Unlock()
	if c.log != nil {
		c.fillLogFields(start, "set", -1).WithField("Key", key).
			WithField("value", value).Info("[ORM][LOCAL][MGET]")
	}
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

	if c.log != nil {
		c.fillLogFields(start, "mset", -1).WithField("Keys", pairs).Info("[ORM][LOCAL][MSET]")
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
	if c.log != nil {
		c.fillLogFields(start, "hmget", misses).
			WithField("Key", key).WithField("fields", fields).Info("[ORM][LOCAL][HMGET]")
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
	if c.log != nil {
		c.fillLogFields(start, "hmset", -1).
			WithField("Key", key).WithField("fields", fields).Info("[ORM][LOCAL][HMSET]")
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
	if c.log != nil {
		c.fillLogFields(start, "remove", -1).
			WithField("Keys", keys).Info("[ORM][LOCAL][REMOVE]")
	}
}

func (c *LocalCache) Clear() {
	start := time.Now()
	m := &sync.Mutex{}
	m.Lock()
	c.lru.Clear()
	m.Unlock()
	if c.log != nil {
		c.fillLogFields(start, "clear", -1).Info("[ORM][LOCAL][CLEAR]")
	}
}

func (c *LocalCache) AddLogger(handler log.Handler) {
	c.logHandler.Handlers = append(c.logHandler.Handlers, handler)
}

func (c *LocalCache) SetLogLevel(level log.Level) {
	logger := log.Logger{Handler: c.logHandler, Level: level}
	c.log = logger.WithField("source", "orm")
	c.log.Level = level
}

func (c *LocalCache) EnableDebug() {
	c.AddLogger(text.New(os.Stdout))
	c.SetLogLevel(log.DebugLevel)
}

func (c *LocalCache) fillLogFields(start time.Time, operation string, misses int) *log.Entry {
	e := c.log.
		WithField("microseconds", time.Since(start).Microseconds()).
		WithField("operation", operation).
		WithField("pool", c.code).
		WithField("target", "local_cache").
		WithField("time", start.Unix())
	if misses >= 0 {
		e = e.WithField("misses", misses)
	}
	return e
}
