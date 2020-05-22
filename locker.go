package orm

import (
	"time"

	"github.com/juju/errors"

	"github.com/bsm/redislock"
)

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

func (l *Locker) Obtain(key string, ttl time.Duration, waitTimeout time.Duration) (lock *Lock, obtained bool, err error) {
	if ttl == 0 {
		return nil, false, errors.Errorf("ttl must be greater than zero")
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
			return nil, false, nil
		}
		if l.engine.loggers[LoggerSourceRedis] != nil {
			l.fillLogFields("[ORM][LOCKER][OBTAIN]", start, key, "obtain", err)
		}
		return nil, false, errors.Trace(err)
	}
	if l.engine.loggers[LoggerSourceRedis] != nil {
		l.fillLogFields("[ORM][LOCKER][OBTAIN]", start, key, "obtain", nil)
	}
	return &Lock{lock: redisLock, locker: l, key: key, has: true, engine: l.engine}, true, nil
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
	if l.engine.loggers[LoggerSourceRedis] != nil {
		l.locker.fillLogFields("[ORM][LOCKER][RELEASE]", start, l.key, "release", err)
	}
	l.has = false
}

func (l *Lock) TTL() (time.Duration, error) {
	start := time.Now()
	d, err := l.lock.TTL()
	if l.engine.loggers[LoggerSourceRedis] != nil {
		l.locker.fillLogFields("[ORM][LOCKER][TTL]", start, l.key, "ttl", err)
	}
	return d, errors.Trace(err)
}

func (l *Locker) fillLogFields(message string, start time.Time, key string, operation string, err error) {
	stop := time.Since(start).Microseconds()
	for _, logger := range l.engine.loggers[LoggerSourceDB] {
		e := logger.log.
			WithField("Key", key).
			WithField("microseconds", stop).
			WithField("operation", operation).
			WithField("pool", l.code).
			WithField("target", "locker").
			WithField("time", start.Unix())
		if err != nil {
			e.WithError(err).Error(message)
		} else {
			e.Info(message)
		}
	}
}
