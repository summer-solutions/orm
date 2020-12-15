package orm

import (
	"strconv"
	"sync"
	"time"
)

type dataLoader struct {
	engine   *Engine
	wait     time.Duration
	maxBatch int
	cache    map[string][]string
	batch    *dataLoaderBatch
	mu       sync.Mutex
}

type dataLoaderBatch struct {
	keys    []string
	data    [][]string
	closing bool
	done    chan struct{}
}

func (l *dataLoader) Load(schema TableSchema, id uint64) []string {
	return l.loadThunk(l.key(schema, id))()
}

func (l *dataLoader) LoadAll(keys []string) [][]string {
	results := make([]func() []string, len(keys))

	for i, key := range keys {
		results[i] = l.loadThunk(key)
	}

	data := make([][]string, len(keys))
	for i, thunk := range results {
		data[i] = thunk()
	}
	return data
}

func (l *dataLoader) LoadAllThunk(keys []string) func() [][]string {
	results := make([]func() []string, len(keys))
	for i, key := range keys {
		results[i] = l.loadThunk(key)
	}
	return func() [][]string {
		data := make([][]string, len(keys))
		for i, thunk := range results {
			data[i] = thunk()
		}
		return data
	}
}

func (l *dataLoader) Prime(schema TableSchema, id uint64, value []string) bool {
	key := l.key(schema, id)
	l.mu.Lock()
	var found bool
	if _, found = l.cache[key]; !found {
		l.unsafeSet(key, value)
	}
	l.mu.Unlock()
	return !found
}

func (l *dataLoader) Clear(key string) {
	l.mu.Lock()
	delete(l.cache, key)
	l.mu.Unlock()
}

func (l *dataLoader) key(schema TableSchema, id uint64) string {
	return schema.GetType().String() + ":" + strconv.FormatUint(id, 10)
}

func (l *dataLoader) loadThunk(key string) func() []string {
	l.mu.Lock()
	if it, ok := l.cache[key]; ok {
		l.mu.Unlock()
		return func() []string {
			return it
		}
	}
	if l.batch == nil {
		l.batch = &dataLoaderBatch{done: make(chan struct{})}
	}
	batch := l.batch
	pos := batch.keyIndex(l, key)
	l.mu.Unlock()

	return func() []string {
		<-batch.done

		var data []string
		if pos < len(batch.data) {
			data = batch.data[pos]
		}

		l.mu.Lock()
		l.unsafeSet(key, data)
		l.mu.Unlock()

		return data
	}
}

func (l *dataLoader) unsafeSet(key string, value []string) {
	if l.cache == nil {
		l.cache = map[string][]string{}
	}
	l.cache[key] = value
}

func (b *dataLoaderBatch) keyIndex(l *dataLoader, key string) int {
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
	// TODO
	//b.data = l.fetch(b.keys)
	close(b.done)
}
