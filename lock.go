package orm

import (
	"fmt"
	"time"

	"github.com/bsm/redislock"
)

type Locker struct {
	locker *redislock.Client
}

func (l *Locker) Obtain(key string, ttl time.Duration, waitTimeout time.Duration) (lock *Lock, obtained bool, err error) {
	if ttl == 0 {
		return nil, false, fmt.Errorf("ttl must be greater than zero")
	}
	if waitTimeout == 0 {
		waitTimeout = ttl
	}
	minInterval := 16*time.Millisecond
	maxInterval := 256*time.Millisecond
	max := int(waitTimeout / maxInterval)
	options := &redislock.Options{RetryStrategy: redislock.LimitRetry(redislock.ExponentialBackoff(minInterval, maxInterval), max)}
	redisLock, err := l.locker.Obtain(key, ttl, options)
	if err != nil {
		if err == redislock.ErrNotObtained {
			return nil, false, nil
		}
		return nil, false, err
	}
	return &Lock{lock: redisLock, has: true}, true, nil
}

type Lock struct {
	lock *redislock.Lock
	has  bool
}

func (l *Lock) Release() error {
	if !l.has {
		return nil
	}
	err := l.lock.Release()
	if err != nil {
		return err
	}
	l.has = false
	return nil
}

func (l *Lock) TTL() (time.Duration, error) {
	return l.lock.TTL()
}
