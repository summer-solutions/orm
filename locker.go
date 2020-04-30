package orm

import (
	"fmt"
	"os"
	"time"

	"github.com/apex/log"
	"github.com/apex/log/handlers/multi"
	"github.com/apex/log/handlers/text"

	"github.com/bsm/redislock"
)

type Locker struct {
	code       string
	locker     *redislock.Client
	log        *log.Entry
	logHandler *multi.Handler
}

func (l *Locker) AddLogger(handler log.Handler) {
	l.logHandler.Handlers = append(l.logHandler.Handlers, handler)
}

func (l *Locker) SetLogLevel(level log.Level) {
	logger := log.Logger{Handler: l.logHandler, Level: level}
	l.log = logger.WithField("source", "orm")
	l.log.Level = level
}

func (l *Locker) EnableDebug() {
	l.AddLogger(text.New(os.Stdout))
	l.SetLogLevel(log.DebugLevel)
}

func (l *Locker) Obtain(key string, ttl time.Duration, waitTimeout time.Duration) (lock *Lock, obtained bool, err error) {
	if ttl == 0 {
		return nil, false, fmt.Errorf("ttl must be greater than zero")
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
	if l.log != nil {
		l.fillLogFields(start, key, "obtain").Info("[ORM][LOCKER][OBTAIN]")
	}
	if err != nil {
		if err == redislock.ErrNotObtained {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &Lock{lock: redisLock, locker: l, key: key, has: true}, true, nil
}

type Lock struct {
	lock   *redislock.Lock
	key    string
	locker *Locker
	has    bool
}

func (l *Lock) Release() {
	if !l.has {
		return
	}
	start := time.Now()
	err := l.lock.Release()
	if l.locker.log != nil {
		l.locker.fillLogFields(start, l.key, "release").Info("[ORM][LOCKER][RELEASE]")
	}
	if err != nil {
		panic(err)
	}
	l.has = false
}

func (l *Lock) TTL() (time.Duration, error) {
	start := time.Now()
	d, err := l.lock.TTL()
	if l.locker.log != nil {
		l.locker.fillLogFields(start, l.key, "ttl").Info("[ORM][LOCKER][TTL]")
	}
	return d, err
}

func (l *Locker) fillLogFields(start time.Time, key string, operation string) *log.Entry {
	return l.log.
		WithField("Key", key).
		WithField("microseconds", time.Since(start).Microseconds()).
		WithField("operation", operation).
		WithField("pool", l.code).
		WithField("target", "locker").
		WithField("time", start.Unix())
}
