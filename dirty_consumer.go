package orm

import (
	"context"
	"time"
)

type DirtyConsumer struct {
	engine            *Engine
	name              string
	group             string
	maxScripts        int
	block             time.Duration
	disableLoop       bool
	heartBeat         func()
	heartBeatDuration time.Duration
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

func NewDirtyConsumer(engine *Engine, name, group string, maxScripts int) *DirtyConsumer {
	return &DirtyConsumer{engine: engine, name: name, group: group, block: time.Minute, maxScripts: maxScripts}
}

func (r *DirtyConsumer) DisableLoop() {
	r.disableLoop = true
}

func (r *DirtyConsumer) SetHeartBeat(duration time.Duration, beat func()) {
	r.heartBeatDuration = duration
	r.heartBeat = beat
}

type DirtyHandler func(data []*DirtyData)

func (r *DirtyConsumer) Digest(ctx context.Context, codes []string, count int, handler DirtyHandler) {
	consumer := r.engine.GetEventBroker().Consumer(r.name, r.group, r.maxScripts, codes...)
	consumer.(*eventsConsumer).block = r.block
	if r.disableLoop {
		consumer.DisableLoop()
	}
	if r.heartBeat != nil {
		consumer.SetHeartBeat(r.heartBeatDuration, r.heartBeat)
	}
	consumer.Consume(ctx, count, func(events []Event) {
		data := make([]*DirtyData, len(events))
		for i, event := range events {
			var value DirtyQueueValue
			err := event.Unserialize(&value)
			if err != nil {
				r.engine.reportError(err)
				continue
			}
			t, has := r.engine.registry.entities[value.EntityName]
			if has {
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
		}
		handler(data)
	})
}
