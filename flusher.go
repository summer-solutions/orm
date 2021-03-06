package orm

import (
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
)

type Flusher interface {
	Track(entity ...Entity) Flusher
	Flush()
	FlushWithCheck() error
	FlushInTransactionWithCheck() error
	FlushWithFullCheck() error
	FlushLazy()
	FlushInTransaction()
	FlushWithLock(lockerPool string, lockName string, ttl time.Duration, waitTimeout time.Duration)
	FlushInTransactionWithLock(lockerPool string, lockName string, ttl time.Duration, waitTimeout time.Duration)
	Clear()
	MarkDirty(entity Entity, queueCode string, ids ...uint64)
	Delete(entity ...Entity) Flusher
	ForceDelete(entity ...Entity) Flusher
}

type flusher struct {
	engine                 *Engine
	trackedEntities        []Entity
	trackedEntitiesCounter int
	mutex                  sync.Mutex
}

func (f *flusher) Track(entity ...Entity) Flusher {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	for _, entity := range entity {
		initIfNeeded(f.engine, entity)
		if f.trackedEntities == nil {
			f.trackedEntities = []Entity{entity}
		} else {
			f.trackedEntities = append(f.trackedEntities, entity)
		}
		f.trackedEntitiesCounter++
		if f.trackedEntitiesCounter == 10001 {
			panic(fmt.Errorf("track limit 10000 exceeded"))
		}
	}
	return f
}

func (f *flusher) Delete(entity ...Entity) Flusher {
	for _, e := range entity {
		e.markToDelete()
	}
	f.Track(entity...)
	return f
}

func (f *flusher) ForceDelete(entity ...Entity) Flusher {
	for _, e := range entity {
		e.forceMarkToDelete()
	}
	f.Track(entity...)
	return f
}

func (f *flusher) Flush() {
	f.flushTrackedEntities(false, false, true)
}

func (f *flusher) FlushWithCheck() error {
	return f.flushWithCheck(false)
}

func (f *flusher) FlushInTransactionWithCheck() error {
	return f.flushWithCheck(true)
}

func (f *flusher) FlushWithFullCheck() error {
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				f.Clear()
				asErr := r.(error)
				err = asErr
			}
		}()
		f.flushTrackedEntities(false, false, false)
	}()
	return err
}

func (f *flusher) FlushLazy() {
	f.flushTrackedEntities(true, false, false)
}

func (f *flusher) FlushInTransaction() {
	f.flushTrackedEntities(false, true, false)
}

func (f *flusher) FlushWithLock(lockerPool string, lockName string, ttl time.Duration, waitTimeout time.Duration) {
	f.flushWithLock(false, lockerPool, lockName, ttl, waitTimeout)
}

func (f *flusher) FlushInTransactionWithLock(lockerPool string, lockName string, ttl time.Duration, waitTimeout time.Duration) {
	f.flushWithLock(true, lockerPool, lockName, ttl, waitTimeout)
}

func (f *flusher) Clear() {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.trackedEntities = nil
	f.trackedEntitiesCounter = 0
}

func (f *flusher) MarkDirty(entity Entity, queueCode string, ids ...uint64) {
	entityName := f.engine.GetRegistry().GetTableSchemaForEntity(entity).GetType().String()
	flusher := f.engine.GetEventBroker().NewFlusher()
	for _, id := range ids {
		flusher.PublishMap(queueCode, EventAsMap{"A": "u", "I": id, "E": entityName})
	}
	flusher.Flush()
}

func (f *flusher) flushTrackedEntities(lazy bool, transaction bool, smart bool) {
	if f.trackedEntitiesCounter == 0 {
		return
	}
	f.mutex.Lock()
	defer f.mutex.Unlock()
	var dbPools map[string]*DB
	if transaction {
		dbPools = make(map[string]*DB)
		for _, entity := range f.trackedEntities {
			db := entity.getORM().tableSchema.GetMysql(f.engine)
			dbPools[db.code] = db
		}
		for _, db := range dbPools {
			db.Begin()
		}
	}
	defer func() {
		for _, db := range dbPools {
			db.Rollback()
		}
	}()
	flush(f.engine, nil, nil, true, lazy, transaction, smart, f.trackedEntities...)
	if transaction {
		for _, db := range dbPools {
			db.Commit()
		}
	}
}

func (f *flusher) flushWithCheck(transaction bool) error {
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				f.Clear()
				asErr := r.(error)
				assErr1, is := asErr.(*ForeignKeyError)
				if is {
					err = assErr1
					return
				}
				assErr2, is := asErr.(*DuplicatedKeyError)
				if is {
					err = assErr2
					return
				}
				panic(asErr)
			}
		}()
		f.flushTrackedEntities(false, transaction, false)
	}()
	return err
}

func (f *flusher) flushWithLock(transaction bool, lockerPool string, lockName string, ttl time.Duration, waitTimeout time.Duration) {
	locker := f.engine.GetLocker(lockerPool)
	lock, has := locker.Obtain(f.engine.context, lockName, ttl, waitTimeout)
	if !has {
		panic(errors.New("lock wait timeout"))
	}
	defer lock.Release()
	f.flushTrackedEntities(false, transaction, false)
}
