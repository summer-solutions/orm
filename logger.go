package orm

import (
	"context"
	"strings"
	"time"

	"github.com/apex/log"
	"github.com/apex/log/handlers/multi"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type LoggerSource int

const (
	LoggerSourceDB = iota
	LoggerSourceRedis
	LoggerSourceRabbitMQ
	LoggerSourceLocalCache
)

type logger struct {
	log     *log.Entry
	handler *multi.Handler
}

func (e *Engine) newLogger(handler log.Handler, level log.Level) *logger {
	multiHandler := multi.New(handler)
	l := log.Logger{Handler: multiHandler, Level: level}
	entry := l.WithField("from", "orm")
	return &logger{log: entry, handler: multiHandler}
}

type dbDataDogHandler struct {
	ctx           context.Context
	withAnalytics bool
	engine        *Engine
}

func newDBDataDogHandler(ctx context.Context, withAnalytics bool, engine *Engine) *dbDataDogHandler {
	return &dbDataDogHandler{ctx, withAnalytics, engine}
}

func (h *dbDataDogHandler) HandleLog(e *log.Entry) error {
	started := time.Unix(0, e.Fields.Get("started").(int64))
	span, _ := tracer.StartSpanFromContext(h.ctx, "mysql.query", tracer.StartTime(started))
	queryType := e.Fields.Get("type")
	span.SetTag(ext.SpanType, ext.SpanTypeSQL)
	span.SetTag(ext.ServiceName, "mysql.db."+e.Fields.Get("pool").(string))
	span.SetTag(ext.ResourceName, e.Fields.Get("Query"))
	span.SetTag(ext.SQLType, queryType)
	if h.withAnalytics {
		span.SetTag(ext.AnalyticsEvent, true)
	}
	injectError(e, span)
	finished := time.Unix(0, e.Fields.Get("finished").(int64))
	span.Finish(tracer.FinishTime(finished))
	if h.engine.dataDog.logDB {
		h.engine.dataDog.dbAll++
		switch queryType {
		case "exec":
			h.engine.dataDog.dbExecs++
		case "transaction":
			h.engine.dataDog.dbTransactions++
		case "select":
			h.engine.dataDog.dbQueries++
		}
	}
	return nil
}

type rabbitMQDataDogHandler struct {
	ctx           context.Context
	withAnalytics bool
	engine        *Engine
}

func newRabbitMQDataDogHandler(ctx context.Context, withAnalytics bool, engine *Engine) *rabbitMQDataDogHandler {
	return &rabbitMQDataDogHandler{ctx, withAnalytics, engine}
}

func (h *rabbitMQDataDogHandler) HandleLog(e *log.Entry) error {
	started := time.Unix(0, e.Fields.Get("started").(int64))
	operationName := e.Fields.Get("operation").(string)
	span, _ := tracer.StartSpanFromContext(h.ctx, operationName, tracer.StartTime(started))
	span.SetTag(ext.SpanType, ext.AppTypeDB)
	span.SetTag(ext.ServiceName, "rabbitMQ.default")
	queue := e.Fields.Get("Queue")
	if queue == nil {
		queue = e.Fields.Get("Router")
	}
	operation := operationName
	if queue != nil {
		operation = operation + " " + queue.(string)
	}
	span.SetTag(ext.ResourceName, operation)
	if h.withAnalytics {
		span.SetTag(ext.AnalyticsEvent, true)
	}
	injectError(e, span)
	finished := time.Unix(0, e.Fields.Get("finished").(int64))
	span.Finish(tracer.FinishTime(finished))
	if h.engine.dataDog.logRabbitMQ {
		h.engine.dataDog.rabbitMQAll++
		switch operationName {
		case "close channel":
			h.engine.dataDog.rabbitMQCloseChannels++
		case "create channel":
			h.engine.dataDog.rabbitMQCreateChannels++
		case "consume":
			h.engine.dataDog.rabbitMQConsumes++
		case "ack":
			h.engine.dataDog.rabbitMQACKs++
		case "receive":
			h.engine.dataDog.rabbitMQReceivers++
		case "connect":
			h.engine.dataDog.rabbitMQConnects++
		case "register":
			h.engine.dataDog.rabbitMQRegisters++
		case "publish":
			h.engine.dataDog.rabbitMQPublished++
		}
	}
	return nil
}

type redisDataDogHandler struct {
	ctx           context.Context
	withAnalytics bool
	engine        *Engine
}

func newRedisDataDogHandler(ctx context.Context, withAnalytics bool, engine *Engine) *redisDataDogHandler {
	return &redisDataDogHandler{ctx, withAnalytics, engine}
}

func (h *redisDataDogHandler) HandleLog(e *log.Entry) error {
	started := time.Unix(0, e.Fields.Get("started").(int64))
	operation := e.Fields.Get("operation").(string)
	span, _ := tracer.StartSpanFromContext(h.ctx, operation, tracer.StartTime(started))
	span.SetTag(ext.SpanType, ext.SpanTypeRedis)
	span.SetTag(ext.ServiceName, "redis."+e.Fields.Get("pool").(string))
	span.SetTag(ext.ResourceName, operation)
	misses := e.Fields.Get("misses")
	if misses != nil {
		span.SetTag("redis.misses", misses)
	}
	keys := e.Fields.Get("keys")
	if keys != nil {
		span.SetTag("redis.keys", keys)
	}
	if h.withAnalytics {
		span.SetTag(ext.AnalyticsEvent, true)
	}
	injectError(e, span)
	finished := time.Unix(0, e.Fields.Get("finished").(int64))
	span.Finish(tracer.FinishTime(finished))
	if h.engine.dataDog.logRedis {
		if e.Fields.Get("target") == "locker" {
			if e.Fields.Get("operation") == "obtain lock" {
				h.engine.dataDog.lockerAll++
			}
		} else {
			keys := uint(e.Fields.Get("keys").(int))
			h.engine.dataDog.redisAll++
			h.engine.dataDog.redisKeys += keys
			misses := e.Fields.Get("misses")
			if misses != nil {
				h.engine.dataDog.redisMisses += uint(misses.(int))
			}
			isSet := e.Fields.Get("is_set")
			isDelete := e.Fields.Get("is_delete")
			if isSet != nil {
				h.engine.dataDog.redisSets++
				h.engine.dataDog.redisSetsKeys += keys
			} else if isDelete != nil {
				h.engine.dataDog.redisDeletes++
				h.engine.dataDog.redisDeletesKeys += keys
			} else {
				h.engine.dataDog.redisGets++
				h.engine.dataDog.redisGetsKeys += keys
			}
		}
	}
	return nil
}

func injectError(e *log.Entry, span tracer.Span) {
	err := e.Fields.Get("error")
	if err != nil {
		span.SetTag(ext.Error, 1)
		span.SetTag(ext.ErrorMsg, err)
		span.SetTag(ext.ErrorDetails, strings.ReplaceAll(e.Fields.Get("stack_full").(string), "\\n", "\n"))
		span.SetTag(ext.ErrorStack, strings.ReplaceAll(e.Fields.Get("stack").(string), "\\n", "\n"))
		span.SetTag(ext.ErrorType, e.Fields.Get("error_type"))
	}
}
