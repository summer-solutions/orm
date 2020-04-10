package orm

type Flusher struct {
	Lazy     bool
	entities []interface{}
}

type AutoFlusher struct {
	Limit        int
	Lazy         bool
	entities     []interface{}
	currentIndex int
}

func (f *Flusher) RegisterEntity(entities ...interface{}) {
	f.entities = append(f.entities, entities...)
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
		f.entities = append(f.entities, entity)
		f.currentIndex = f.currentIndex + 1
	}
	return nil
}

func (f *Flusher) Flush(engine *Engine) error {
	err := flush(engine, f.Lazy, f.entities...)
	if err != nil {
		return err
	}
	f.entities = make([]interface{}, 0)
	return nil
}

func (f *Flusher) FlushAndReturn(engine *Engine) ([]interface{}, error) {
	err := flush(engine, f.Lazy, f.entities...)
	if err != nil {
		return nil, err
	}
	entities := f.entities
	f.entities = make([]interface{}, 0)
	return entities, nil
}

func (f *AutoFlusher) Flush(engine *Engine) error {
	err := flush(engine, f.Lazy, f.entities...)
	if err != nil {
		return err
	}
	f.currentIndex = 0
	f.entities = make([]interface{}, 0)
	return nil
}
