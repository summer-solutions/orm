package orm

import (
	jsoniter "github.com/json-iterator/go"
)

type DirtyReceiver struct {
	engine      *Engine
	disableLoop bool
}

type DirtyQueueValue struct {
	EntityName string
	ID         uint64
	Added      bool
	Updated    bool
	Deleted    bool
}

type DirtyData struct {
	TableSchema *tableSchema
	ID          uint64
	Added       bool
	Updated     bool
	Deleted     bool
}

func NewDirtyReceiver(engine *Engine) *DirtyReceiver {
	return &DirtyReceiver{engine: engine}
}

func (r *DirtyReceiver) DisableLoop() {
	r.disableLoop = true
}

type DirtyHandler func(data []*DirtyData) error

func (r *DirtyReceiver) Digest(code string, handler DirtyHandler) error {
	channel := r.engine.GetRabbitMQQueue("dirty_queue_" + code)
	consumer, err := channel.NewConsumer("default consumer")
	if err != nil {
		return err
	}
	defer consumer.Close()
	if r.disableLoop {
		consumer.DisableLoop()
	}
	var value DirtyQueueValue
	err = consumer.Consume(func(items [][]byte) error {
		data := make([]*DirtyData, len(items))
		for i, item := range items {
			_ = jsoniter.ConfigFastest.Unmarshal(item, &value)
			t, has := r.engine.registry.entities[value.EntityName]
			if !has {
				return nil
			}
			tableSchema := getTableSchema(r.engine.registry, t)
			v := &DirtyData{
				TableSchema: tableSchema,
				ID:          value.ID,
				Added:       value.Added,
				Updated:     value.Updated,
				Deleted:     value.Deleted,
			}
			data[i] = v
		}
		return handler(data)
	})
	if err != nil {
		return err
	}
	return nil
}
