package orm

import (
	"sync"
	"time"
)

type dataLoaderConfig struct {
	Fetch    func(keys []uint64) ([]*Entity, error)
	Wait     time.Duration
	MaxBatch int
}

func newDataLoader(config dataLoaderConfig) *dataLoader {
	return &dataLoader{
		fetch:    config.Fetch,
		wait:     config.Wait,
		maxBatch: config.MaxBatch,
	}
}

type dataLoader struct {
	fetch    func(keys []uint64) ([]*Entity, error)
	wait     time.Duration
	maxBatch int
	cache    map[uint64]*Entity
	batch    *dataLoaderBatch
	mu       sync.Mutex
}

type dataLoaderBatch struct {
	keys    []uint64
	data    []*Entity
	error   error
	closing bool
	done    chan struct{}
}

func (l *dataLoader) Load(key uint64) (*Entity, error) {
	return l.LoadThunk(key)()
}

func (l *dataLoader) LoadThunk(key uint64) func() (*Entity, error) {
	l.mu.Lock()
	if it, ok := l.cache[key]; ok {
		l.mu.Unlock()
		return func() (*Entity, error) {
			return it, nil
		}
	}
	if l.batch == nil {
		l.batch = &dataLoaderBatch{done: make(chan struct{})}
	}
	batch := l.batch
	pos := batch.keyIndex(l, key)
	l.mu.Unlock()

	return func() (*Entity, error) {
		<-batch.done

		var data *Entity
		if pos < len(batch.data) {
			data = batch.data[pos]
		}

		err := batch.error

		if err == nil {
			l.mu.Lock()
			l.unsafeSet(key, data)
			l.mu.Unlock()
		}

		return data, err
	}
}

func (l *dataLoader) LoadAll(keys []uint64) ([]*Entity, []error) {
	results := make([]func() (*Entity, error), len(keys))

	for i, key := range keys {
		results[i] = l.LoadThunk(key)
	}

	carBrands := make([]*Entity, len(keys))
	errors := make([]error, len(keys))
	for i, thunk := range results {
		carBrands[i], errors[i] = thunk()
	}
	return carBrands, errors
}

func (l *dataLoader) LoadAllThunk(keys []uint64) func() ([]*Entity, []error) {
	results := make([]func() (*Entity, error), len(keys))
	for i, key := range keys {
		results[i] = l.LoadThunk(key)
	}
	return func() ([]*Entity, []error) {
		carBrands := make([]*Entity, len(keys))
		errors := make([]error, len(keys))
		for i, thunk := range results {
			carBrands[i], errors[i] = thunk()
		}
		return carBrands, errors
	}
}

func (l *dataLoader) Prime(key uint64, value *Entity) bool {
	l.mu.Lock()
	var found bool
	if _, found = l.cache[key]; !found {
		cpy := *value
		l.unsafeSet(key, &cpy)
	}
	l.mu.Unlock()
	return !found
}

func (l *dataLoader) Clear(key uint64) {
	l.mu.Lock()
	delete(l.cache, key)
	l.mu.Unlock()
}

func (l *dataLoader) unsafeSet(key uint64, value *Entity) {
	if l.cache == nil {
		l.cache = map[uint64]*Entity{}
	}
	l.cache[key] = value
}

func (b *dataLoaderBatch) keyIndex(l *dataLoader, key uint64) int {
	for i, existingKey := range b.keys {
		if key == existingKey {
			return i
		}
	}

	pos := len(b.keys)
	b.keys = append(b.keys, key)
	if pos == 0 {
		go b.startTimer(l)
	}

	if l.maxBatch != 0 && pos >= l.maxBatch-1 {
		if !b.closing {
			b.closing = true
			l.batch = nil
			go b.end(l)
		}
	}

	return pos
}

func (b *dataLoaderBatch) startTimer(l *dataLoader) {
	time.Sleep(l.wait)
	l.mu.Lock()

	if b.closing {
		l.mu.Unlock()
		return
	}

	l.batch = nil
	l.mu.Unlock()

	b.end(l)
}

func (b *dataLoaderBatch) end(l *dataLoader) {
	b.data, b.error = l.fetch(b.keys)
	close(b.done)
}
