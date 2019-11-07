package orm

import "fmt"

type Flusher struct {
	limit        int
	autoFlush    bool
	entities     []interface{}
	currentIndex int
}

func NewFlusher(limit int, autoFlush bool) *Flusher {
	return &Flusher{limit: limit, autoFlush: autoFlush, entities: make([]interface{}, 0, limit)}
}

func (f *Flusher) RegisterEntity(entities ...interface{}) {
	for _, entity := range entities {
		if f.currentIndex == f.limit {
			if !f.autoFlush {
				panic(fmt.Errorf("flusher limit %d exceeded", entity))
			}
			err := f.Flush()
			if err != nil {
				panic(err.Error())
			}
		}
		f.entities = append(f.entities, entity)
		f.currentIndex = f.currentIndex + 1
	}
}

func (f *Flusher) Flush() error {
	return f.flush(false)
}

func (f *Flusher) FlushLazy() error {
	return f.flush(true)
}

func (f *Flusher) flush(lazy bool) error {
	err := flush(lazy, false, f.entities...)
	if err != nil {
		return err
	}
	f.currentIndex = 0
	f.entities = make([]interface{}, 0, f.limit)
	return nil
}
