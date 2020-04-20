package orm

import "reflect"

type Flusher struct {
	Lazy     bool
	values   []reflect.Value
	entities []interface{}
}

type AutoFlusher struct {
	Limit        int
	Lazy         bool
	values       []reflect.Value
	entities     []interface{}
	currentIndex int
}

func (f *Flusher) RegisterEntity(entities ...interface{}) {
	for _, entity := range entities {
		f.entities = append(f.entities, entity)
		f.values = append(f.values, reflect.ValueOf(entity))
	}
}

func (f *Flusher) GetEntities() []interface{} {
	return f.entities
}

func (f *AutoFlusher) RegisterEntity(engine *Engine, entities ...interface{}) error {
	if f.Limit == 0 {
		f.Limit = 10000
	}
	for _, entity := range entities {
		if f.currentIndex == f.Limit {
			err := f.Flush(engine)
			if err != nil {
				return err
			}
		}
		f.values = append(f.values, reflect.ValueOf(entity))
		f.entities = append(f.entities, entity)
		f.currentIndex = f.currentIndex + 1
	}
	return nil
}

func (f *Flusher) Flush(engine *Engine) error {
	err := flush(engine, f.Lazy, f.values...)
	if err != nil {
		return err
	}
	f.entities = make([]interface{}, 0)
	f.values = make([]reflect.Value, 0)
	return nil
}

func (f *AutoFlusher) Flush(engine *Engine) error {
	err := flush(engine, f.Lazy, f.values...)
	if err != nil {
		return err
	}
	f.currentIndex = 0
	f.entities = make([]interface{}, 0)
	f.values = make([]reflect.Value, 0)
	return nil
}

func (f *AutoFlusher) GetEntities() []interface{} {
	return f.entities
}
