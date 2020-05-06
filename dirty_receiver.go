package orm

import (
	jsoniter "github.com/json-iterator/go"
)

type DirtyReceiver struct {
	engine *Engine
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

type DirtyHandler func(data *DirtyData, queueName string) error

func (r *DirtyReceiver) Digest(queueName string, item []byte, handler DirtyHandler) error {
	var value DirtyQueueValue
	err := jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal(item, &value)
	if err != nil {
		return nil
	}
	t, has := r.engine.registry.entities[value.EntityName]
	if !has {
		return nil
	}
	tableSchema := getTableSchema(r.engine.registry, t)
	data := &DirtyData{
		TableSchema: tableSchema,
		ID:          value.ID,
		Added:       value.Added,
		Updated:     value.Updated,
		Deleted:     value.Deleted,
	}
	return handler(data, queueName)
}
