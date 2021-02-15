package orm

import (
	"context"
	"time"

	"github.com/pkg/errors"

	log2 "github.com/apex/log"

	"github.com/bsm/redislock"
)

type lockerClient interface {
	Obtain(ctx context.Context, key string, ttl time.Duration, opt *redislock.Options) (*redislock.Lock, error)
}

type standardLockerClient struct {
	client *redislock.Client
}

func (l *standardLockerClient) Obtain(ctx context.Context, key string, ttl time.Duration, opt *redislock.Options) (*redislock.Lock, error) {
	return l.client.Obtain(ctx, key, ttl, opt)
}

type Locker struct {
	code   string
	locker lockerClient
	engine *Engine
}

func (l *Locker) Obtain(ctx context.Context, key string, ttl time.Duration, waitTimeout time.Duration) (lock *Lock, obtained bool) {
	if ttl == 0 {
		panic(errors.New("ttl must be higher than zero"))
	}
	var options *redislock.Options
	if waitTimeout > 0 {
		minInterval := 16 * time.Millisecond
		maxInterval := 256 * time.Millisecond
		max := int(waitTimeout / maxInterval)
		if max == 0 {
			max = 1
		}
		options = &redislock.Options{RetryStrategy: redislock.LimitRetry(redislock.ExponentialBackoff(minInterval, maxInterval), max)}
	}
	start := time.Now()
	redisLock, err := l.locker.Obtain(ctx, key, ttl, options)
	if err != nil {
		if err == redislock.ErrNotObtained {
			if l.engine.hasRedisLogger {
				l.fillLogFields("[ORM][LOCKER][OBTAIN]", start, key, "obtain lock", nil,
					log2.Fields{"ttl": ttl.String(), "waitTimeout": waitTimeout.String()})
			}
			return nil, false
		}
	}
	if l.engine.hasRedisLogger {
		l.fillLogFields("[ORM][LOCKER][OBTAIN]", start, key, "obtain lock", err,
			log2.Fields{"ttl": ttl.String(), "waitTimeout": waitTimeout.String()})
	}
	checkError(err)
	lock = &Lock{lock: redisLock, locker: l, key: key, has: true, engine: l.engine}
	lock.timer = time.NewTimer(ttl)
	lock.done = make(chan bool)
	go func() {
		for {
			select {
			case <-ctx.Done():
				lock.Release()
				return
			case <-lock.timer.C:
				return
			case <-lock.done:
				return
			}
		}
	}()
	return lock, true
}

type Lock struct {
	lock   *redislock.Lock
	key    string
	locker *Locker
	has    bool
	engine *Engine
	timer  *time.Timer
	done   chan bool
}

func (l *Lock) Release() {
	if !l.has {
		return
	}
	l.has = false
	start := time.Now()
	err := l.lock.Release(l.engine.context)
	if err == redislock.ErrLockNotHeld {
		err = nil
	}
	if l.engine.hasRedisLogger {
		l.locker.fillLogFields("[ORM][LOCKER][RELEASE]", start, l.key, "release lock", err, nil)
	}
	close(l.done)
}

func (l *Lock) TTL() time.Duration {
	start := time.Now()
	d, err := l.lock.TTL(l.engine.context)
	if l.engine.hasRedisLogger {
		l.locker.fillLogFields("[ORM][LOCKER][TTL]", start, l.key, "ttl lock", err, nil)
	}
	checkError(err)
	return d
}

func (l *Lock) Refresh(ctx context.Context, ttl time.Duration) bool {
	if !l.has {
		return false
	}
	start := time.Now()
	err := l.lock.Refresh(ctx, ttl, nil)
	has := true
	if err == redislock.ErrNotObtained {
		has = false
		err = nil
		l.has = false
	}
	l.timer.Reset(ttl)
	if l.engine.hasRedisLogger {
		l.locker.fillLogFields("[ORM][LOCKER][REFRESH]", start,
			l.key, "refresh lock", err, log2.Fields{"ttl": ttl.String()})
	}
	checkError(err)
	return has
}

func (l *Locker) fillLogFields(message string, start time.Time, key string, operation string, err error, extra log2.Fields) {
	now := time.Now()
	stop := time.Since(start).Microseconds()
	e := l.engine.queryLoggers[QueryLoggerSourceRedis].log.WithFields(log2.Fields{
		"Key":          key,
		"microseconds": stop,
		"operation":    operation,
		"pool":         l.code,
		"target":       "locker",
		"started":      start.UnixNano(),
		"finished":     now.UnixNano(),
	})
	if extra != nil {
		e = e.WithFields(extra)
	}
	if err != nil {
		injectLogError(err, e).Error(message)
	} else {
		e.Info(message)
	}
}
