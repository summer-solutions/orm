package orm

import (
	"context"
	"encoding/json"
	"time"

	"github.com/go-redis/redis/v8"
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
	consumer := r.engine.GetRedis().NewStreamGroupConsumer(r.name, r.group, r.maxScripts, codes...)
	consumer.(*redisStreamGroupConsumer).block = r.block
	if r.disableLoop {
		consumer.DisableLoop()
	}
	if r.heartBeat != nil {
		consumer.SetHeartBeat(r.heartBeatDuration, r.heartBeat)
	}
	consumer.Consume(ctx, count, func(streams []redis.XStream, ack *RedisStreamGroupAck) {
		for _, stream := range streams {
			data := make([]*DirtyData, len(stream.Messages))
			for i, item := range stream.Messages {
				var value DirtyQueueValue
				_ = json.Unmarshal([]byte(item.Values["v"].(string)), &value)
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
			ack.Ack(stream.Stream, stream.Messages...)
		}
	})
}
