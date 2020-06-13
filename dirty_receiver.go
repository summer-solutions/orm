package orm

import (
	"encoding/json"
	"time"
)

type DirtyReceiver struct {
	engine          *Engine
	disableLoop     bool
	heartBeat       func()
	maxLoopDuration time.Duration
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

func (r *DirtyReceiver) SetHeartBeat(beat func()) {
	r.heartBeat = beat
}

func (r *DirtyReceiver) SetMaxLoopDuration(duration time.Duration) {
	r.maxLoopDuration = duration
}

type DirtyHandler func(data []*DirtyData)

func (r *DirtyReceiver) Purge(code string) {
	channel := r.engine.GetRabbitMQQueue("dirty_queue_" + code)
	consumer := channel.NewConsumer("default consumer")
	consumer.Purge()
}

func (r *DirtyReceiver) Digest(code string, handler DirtyHandler) {
	channel := r.engine.GetRabbitMQQueue("dirty_queue_" + code)
	consumer := channel.NewConsumer("default consumer")
	defer consumer.Close()
	if r.disableLoop {
		consumer.DisableLoop()
	}
	if r.heartBeat != nil {
		consumer.SetHeartBeat(r.heartBeat)
	}
	if r.maxLoopDuration > 0 {
		consumer.SetMaxLoopDuration(r.maxLoopDuration)
	}
	var value DirtyQueueValue
	consumer.Consume(func(items [][]byte) {
		data := make([]*DirtyData, len(items))
		for i, item := range items {
			_ = json.Unmarshal(item, &value)
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
