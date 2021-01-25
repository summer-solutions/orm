package orm

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
)

type RedisPipeLine struct {
	engine   *Engine
	pool     string
	pipeLine redis.Pipeliner
	ctx      context.Context
	executed bool
	commands int
}

func (rp *RedisPipeLine) Get(key string) *PipeLineGet {
	rp.commands++
	return &PipeLineGet{p: rp, cmd: rp.pipeLine.Get(rp.ctx, key)}
}

func (rp *RedisPipeLine) Set(key string, value interface{}, expiration time.Duration) *PipeLineStatus {
	rp.commands++
	return &PipeLineStatus{p: rp, cmd: rp.pipeLine.Set(rp.ctx, key, value, expiration)}
}

func (rp *RedisPipeLine) XAdd(stream string, values interface{}) *PipeLineString {
	rp.commands++
	return &PipeLineString{p: rp, cmd: rp.pipeLine.XAdd(rp.ctx, &redis.XAddArgs{Stream: stream, Values: values})}
}

func (rp *RedisPipeLine) Exec() {
	if rp.executed {
		panic(fmt.Errorf("pipeline is already executed"))
	}
	start := time.Now()
	_, err := rp.pipeLine.Exec(rp.ctx)
	rp.executed = true
	if err != nil && err == redis.Nil {
		err = nil
	}
	if rp.engine.queryLoggers[QueryLoggerSourceRedis] != nil {
		rp.fillLogFields(start, err)
	}
	checkError(err)
}

func (rp *RedisPipeLine) Executed() bool {
	return rp.executed
}

type PipeLineGet struct {
	p   *RedisPipeLine
	cmd *redis.StringCmd
}

func (c *PipeLineGet) Result() (value string, has bool, err error) {
	checkExecuted(c.p)
	val, err := c.cmd.Result()
	if err == redis.Nil {
		return val, false, nil
	}
	return val, true, err
}

type PipeLineString struct {
	p   *RedisPipeLine
	cmd *redis.StringCmd
}

func (c *PipeLineString) Result() (string, error) {
	checkExecuted(c.p)
	return c.cmd.Result()
}

type PipeLineStatus struct {
	p   *RedisPipeLine
	cmd *redis.StatusCmd
}

func (c *PipeLineStatus) Result() error {
	checkExecuted(c.p)
	_, err := c.cmd.Result()
	return err
}

func (rp *RedisPipeLine) fillLogFields(start time.Time, err error) {
	message := "[ORM][REDIS][EXEC]"
	now := time.Now()
	stop := time.Since(start).Microseconds()
	e := rp.engine.queryLoggers[QueryLoggerSourceRedis].log.
		WithField("microseconds", stop).
		WithField("operation", "exec").
		WithField("commands", rp.commands).
		WithField("pool", rp.pool).
		WithField("target", "redis").
		WithField("started", start.UnixNano()).
		WithField("finished", now.UnixNano())
	if err != nil {
		injectLogError(err, e).Error(message)
	} else {
		e.Info(message)
	}
}

func checkExecuted(p *RedisPipeLine) {
	if !p.Executed() {
		panic(fmt.Errorf("pipeline must be executed first"))
	}
}
