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
	ctx context.Context
}

func newDBDataDogHandler(ctx context.Context) *dbDataDogHandler {
	return &dbDataDogHandler{ctx}
}

func (h *dbDataDogHandler) HandleLog(e *log.Entry) error {
	started := time.Unix(0, e.Fields.Get("started").(int64))
	span, _ := tracer.StartSpanFromContext(h.ctx, "mysql.query", tracer.StartTime(started))
	span.SetTag(ext.SpanType, ext.SpanTypeSQL)
	span.SetTag(ext.ServiceName, "mysql.db."+e.Fields.Get("pool").(string))
	span.SetTag(ext.ResourceName, e.Fields.Get("Query"))
	span.SetTag(ext.SQLType, e.Fields.Get("type"))
	injectError(e, span)
	finished := time.Unix(0, e.Fields.Get("finished").(int64))
	span.Finish(tracer.FinishTime(finished))
	return nil
}

type rabbitMQDataDogHandler struct {
	ctx context.Context
}

func newRabbitMQDataDogHandler(ctx context.Context) *rabbitMQDataDogHandler {
	return &rabbitMQDataDogHandler{ctx}
}

func (h *rabbitMQDataDogHandler) HandleLog(e *log.Entry) error {
	started := time.Unix(0, e.Fields.Get("started").(int64))
	operation := e.Fields.Get("operation").(string)
	span, _ := tracer.StartSpanFromContext(h.ctx, operation, tracer.StartTime(started))
	span.SetTag(ext.SpanType, ext.AppTypeDB)
	span.SetTag(ext.ServiceName, "rabbitMQ.default")
	queue := e.Fields.Get("Queue")
	if queue == nil {
		queue = e.Fields.Get("Router")
	}
	if queue != nil {
		operation = operation + " " + queue.(string)
	}
	span.SetTag(ext.ResourceName, operation)
	injectError(e, span)
	finished := time.Unix(0, e.Fields.Get("finished").(int64))
	span.Finish(tracer.FinishTime(finished))
	return nil
}

type redisDataDogHandler struct {
	ctx context.Context
}

func newRedisDataDogHandler(ctx context.Context) *redisDataDogHandler {
	return &redisDataDogHandler{ctx}
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
	injectError(e, span)
	finished := time.Unix(0, e.Fields.Get("finished").(int64))
	span.Finish(tracer.FinishTime(finished))
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
