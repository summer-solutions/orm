package orm

import (
	"context"
	"strconv"
	"time"

	"github.com/pkg/errors"
)

type RedisSearchIndexer struct {
	engine            *Engine
	disableLoop       bool
	heartBeat         func()
	heartBeatDuration time.Duration
}

func NewRedisSearchIndexer(engine *Engine) *RedisSearchIndexer {
	return &RedisSearchIndexer{engine: engine}
}

func (r *RedisSearchIndexer) DisableLoop() {
	r.disableLoop = true
}

func (r *RedisSearchIndexer) SetHeartBeat(duration time.Duration, beat func()) {
	r.heartBeatDuration = duration
	r.heartBeat = beat
}

func (r *RedisSearchIndexer) Run(ctx context.Context) {
	for {
		valid := r.consume(ctx)
		if valid || r.disableLoop {
			break
		}
		time.Sleep(time.Second * 10)
	}
}

func (r *RedisSearchIndexer) consume(ctx context.Context) bool {
	canceled := false
	go func() {
		<-ctx.Done()
		canceled = true
	}()
	for {
		for pool, defs := range r.engine.registry.redisSearchIndexes {
			search := r.engine.GetRedisSearch(pool)
			stamps := search.redis.HGetAll(redisSearchForceIndexKey)
			for index, stamp := range stamps {
				if canceled {
					return true
				}
				def, has := defs[index]
				if !has || def.Indexer == nil {
					search.redis.HDel(redisSearchForceIndexKey, index)
					continue
				}
				if stamp == "ok" {
					continue
				}
				id, _ := strconv.ParseUint(stamp, 10, 64)
				pusher := &redisSearchIndexPusher{pipeline: search.redis.PipeLine()}
				for {
					if canceled {
						return true
					}
					nextID, hasMore := def.Indexer(id, pusher)
					if pusher.pipeline.commands > 0 {
						pusher.pipeline.Exec()
						pusher.pipeline = search.redis.PipeLine()
					}
					if !hasMore {
						search.redis.HSet(redisSearchForceIndexKey, index, "ok")
						break
					}
					if nextID <= id {
						panic(errors.Errorf("loop detected in indxer for index %s in pool %s", index, pool))
					}
					id = nextID
				}
			}
		}
		if r.disableLoop {
			break
		}
		time.Sleep(time.Second * 15)
	}
	return true
}

type RedisSearchIndexPusher interface {
	NewDocument(key string)
	SetField(key string, value interface{})
	PushDocument()
}

type RedisSearchIndexerFunc func(lastID uint64, pusher RedisSearchIndexPusher) (newID uint64, hasMore bool)

type redisSearchIndexPusher struct {
	pipeline *RedisPipeLine
	key      string
	fields   []interface{}
}

func (p *redisSearchIndexPusher) NewDocument(key string) {
	p.key = key
}

func (p *redisSearchIndexPusher) SetField(key string, value interface{}) {
	p.fields = append(p.fields, key, value)
}

func (p *redisSearchIndexPusher) PushDocument() {
	p.pipeline.HSet(p.key, p.fields...)
	p.key = ""
	p.fields = p.fields[:0]
}
