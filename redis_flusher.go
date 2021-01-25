package orm

import (
	"sync"

	jsoniter "github.com/json-iterator/go"
)

const (
	commandDelete = iota
	commandXAdd   = iota
)

type RedisFlusher interface {
	Del(redisPool string, keys ...string)
	PublishMap(stream string, event EventAsMap)
	Publish(stream string, event interface{})
	Flush()
}

type redisFlusherCommands struct {
	diffs   map[int]bool
	deletes []string
	events  []EventAsMap
}

type redisFlusher struct {
	engine    *Engine
	mutex     sync.Mutex
	pipelines map[string]*redisFlusherCommands
}

func (f *redisFlusher) Del(redisPool string, keys ...string) {
	if len(keys) == 0 {
		return
	}
	f.mutex.Lock()
	defer f.mutex.Unlock()
	commands, has := f.pipelines[redisPool]
	if !has {
		commands = &redisFlusherCommands{deletes: keys, diffs: map[int]bool{commandDelete: true}}
		f.pipelines[redisPool] = commands
		return
	}
	commands.diffs[commandDelete] = true
	if commands.deletes == nil {
		commands.deletes = keys
		return
	}
	commands.deletes = append(commands.deletes, keys...)
}

func (f *redisFlusher) PublishMap(stream string, event EventAsMap) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	r := getRedisForStream(f.engine, stream)
	commands, has := f.pipelines[r.code]
	if !has {
		commands = &redisFlusherCommands{events: []EventAsMap{event}, diffs: map[int]bool{commandXAdd: true}}
		f.pipelines[r.code] = commands
		return
	}
	commands.diffs[commandXAdd] = true
	if commands.events == nil {
		commands.events = []EventAsMap{event}
		return
	}
	commands.events = append(commands.events, event)
}

func (f *redisFlusher) Publish(stream string, event interface{}) {
	asJSON, err := jsoniter.ConfigFastest.Marshal(event)
	if err != nil {
		panic(err)
	}
	f.PublishMap(stream, EventAsMap{"_s": string(asJSON)})
}

func (f *redisFlusher) Flush() {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	for poolCode, commands := range f.pipelines {
		if len(commands.diffs) == 1 {
			r := f.engine.GetRedis(poolCode)
			if commands.deletes != nil {
				r.Del(commands.deletes...)
			}
		} else {
			p := f.engine.GetRedis(poolCode).PipeLine()
			if commands.deletes != nil {
				p.Del(commands.deletes...)
			}
			p.Exec()
		}
	}
	f.pipelines = nil
}
