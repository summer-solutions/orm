package orm

import "fmt"

type Flusher struct {
	Limit        int
	Lazy         bool
	entities     []interface{}
	currentIndex int
}

type AutoFlusher struct {
	Limit        int
	Lazy         bool
	entities     []interface{}
	currentIndex int
}

func (f *Flusher) RegisterEntity(entities ...interface{}) {
	if f.Limit == 0 {
		f.Limit = 10000
	}
	for _, entity := range entities {
		if f.currentIndex == f.Limit {
			panic(fmt.Errorf("flusher limit %d exceeded", f.Limit))
		}
		f.entities = append(f.entities, entity)
		f.currentIndex = f.currentIndex + 1
	}
}

func (f *AutoFlusher) RegisterEntity(entities ...interface{}) error {
	if f.Limit == 0 {
		f.Limit = 10000
	}
	for _, entity := range entities {
		if f.currentIndex == f.Limit {
			err := f.Flush()
			if err != nil {
				return err
			}
		}
		f.entities = append(f.entities, entity)
		f.currentIndex = f.currentIndex + 1
	}
	return nil
}

func (f *Flusher) Flush() error {
	err := flush(f.Lazy, f.entities...)
	if err != nil {
		return err
	}
	f.currentIndex = 0
	f.entities = make([]interface{}, 0)
	return nil
}

func (f *AutoFlusher) Flush() error {
	err := flush(f.Lazy, f.entities...)
	if err != nil {
		return err
	}
	f.currentIndex = 0
	f.entities = make([]interface{}, 0)
	return nil
}
