package orm

import (
	"time"

	"github.com/juju/errors"

	"github.com/bsm/redislock"
)

const counterRedisLockObtain = "redis.lockObtain"
const counterRedisLockRelease = "redis.lockRelease"
const counterRedisLockTTL = "redis.lockTTL"

type lockerClient interface {
	Obtain(key string, ttl time.Duration, opt *redislock.Options) (*redislock.Lock, error)
}

type standardLockerClient struct {
	client *redislock.Client
}

func (l *standardLockerClient) Obtain(key string, ttl time.Duration, opt *redislock.Options) (*redislock.Lock, error) {
	return l.client.Obtain(key, ttl, opt)
}

type Locker struct {
	code   string
	locker lockerClient
	engine *Engine
}

func (l *Locker) Obtain(key string, ttl time.Duration, waitTimeout time.Duration) (lock *Lock, obtained bool) {
	if ttl == 0 {
		panic(errors.NotValidf("ttl must be greater than zero"))
	}
	if waitTimeout == 0 {
		waitTimeout = ttl
	}
	minInterval := 16 * time.Millisecond
	maxInterval := 256 * time.Millisecond
	max := int(waitTimeout / maxInterval)
	options := &redislock.Options{RetryStrategy: redislock.LimitRetry(redislock.ExponentialBackoff(minInterval, maxInterval), max)}
	start := time.Now()
	redisLock, err := l.locker.Obtain(key, ttl, options)
	if err != nil {
		if err == redislock.ErrNotObtained {
			return nil, false
		}
	}
	if l.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		l.fillLogFields("[ORM][LOCKER][OBTAIN]", start, key, "obtain lock", err)
	}
	checkError(err)
	l.engine.dataDog.incrementCounter(counterRedisAll, 1)
	l.engine.dataDog.incrementCounter(counterRedisLockObtain, 1)
	return &Lock{lock: redisLock, locker: l, key: key, has: true, engine: l.engine}, true
}

type Lock struct {
	lock   *redislock.Lock
	key    string
	locker *Locker
	has    bool
	engine *Engine
}

func (l *Lock) Release() {
	if !l.has {
		return
	}
	start := time.Now()
	err := l.lock.Release()
	if l.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		l.locker.fillLogFields("[ORM][LOCKER][RELEASE]", start, l.key, "release lock", err)
	}
	l.engine.dataDog.incrementCounter(counterRedisAll, 1)
	l.engine.dataDog.incrementCounter(counterRedisLockRelease, 1)
	l.has = false
}

func (l *Lock) TTL() time.Duration {
	start := time.Now()
	d, err := l.lock.TTL()
	if l.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		l.locker.fillLogFields("[ORM][LOCKER][TTL]", start, l.key, "ttl lock", err)
	}
	l.engine.dataDog.incrementCounter(counterRedisAll, 1)
	l.engine.dataDog.incrementCounter(counterRedisLockTTL, 1)
	checkError(err)
	return d
}

func (l *Locker) fillLogFields(message string, start time.Time, key string, operation string, err error) {
	now := time.Now()
	stop := time.Since(start).Microseconds()
	e := l.engine.queryLoggers[QueryLoggerSourceRedis].log.
		WithField("Key", key).
		WithField("microseconds", stop).
		WithField("operation", operation).
		WithField("pool", l.code).
		WithField("target", "locker").
		WithField("started", start.UnixNano()).
		WithField("finished", now.UnixNano())
	if err != nil {
		injectLogError(err, e).Error(message)
	} else {
		e.Info(message)
	}
}
