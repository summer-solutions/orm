package orm

import (
	"github.com/bsm/redislock"
	"time"
)

type Locker struct {
	locker *redislock.Client
}

func (l *Locker) Obtain(key string, ttl time.Duration) (lock *Lock, obtained bool, err error) {
	options := &redislock.Options{RetryStrategy: redislock.ExponentialBackoff(16, 1000)}
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
