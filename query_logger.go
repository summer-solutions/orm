package orm

import (
	"reflect"
	"runtime/debug"
	"strings"
	"time"

	"github.com/juju/errors"

	apexLox "github.com/apex/log"

	"github.com/apex/log/handlers/multi"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type QueryLoggerSource int

const (
	QueryLoggerSourceDB = iota
	QueryLoggerSourceRedis
	QueryLoggerSourceRabbitMQ
	QueryLoggerSourceElastic
	QueryLoggerSourceClickHouse
	QueryLoggerSourceLocalCache
)

type logger struct {
	log     *apexLox.Entry
	handler *multi.Handler
}

func (e *Engine) newLogger(handler apexLox.Handler, level apexLox.Level) *logger {
	multiHandler := multi.New(handler)
	l := apexLox.Logger{Handler: multiHandler, Level: level}
	entry := l.WithField("from", "orm")
	return &logger{log: entry, handler: multiHandler}
}

type dbDataDogHandler struct {
	withAnalytics bool
	engine        *Engine
}

func newDBDataDogHandler(withAnalytics bool, engine *Engine) *dbDataDogHandler {
	return &dbDataDogHandler{withAnalytics, engine}
}

func (h *dbDataDogHandler) HandleLog(e *apexLox.Entry) error {
	l := len(h.engine.dataDog.ctx)
	if l == 0 {
		return nil
	}
	started := time.Unix(0, e.Fields.Get("started").(int64))
	span, _ := tracer.StartSpanFromContext(h.engine.dataDog.ctx[l-1], "mysql.query", tracer.StartTime(started))
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
	return nil
}

type rabbitMQDataDogHandler struct {
	withAnalytics bool
	engine        *Engine
}

func newRabbitMQDataDogHandler(withAnalytics bool, engine *Engine) *rabbitMQDataDogHandler {
	return &rabbitMQDataDogHandler{withAnalytics, engine}
}

func (h *rabbitMQDataDogHandler) HandleLog(e *apexLox.Entry) error {
	l := len(h.engine.dataDog.ctx)
	if l == 0 {
		return nil
	}
	started := time.Unix(0, e.Fields.Get("started").(int64))
	operationName := e.Fields.Get("operation").(string)
	span, _ := tracer.StartSpanFromContext(h.engine.dataDog.ctx[l-1], operationName, tracer.StartTime(started))
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
	return nil
}

type redisDataDogHandler struct {
	withAnalytics bool
	engine        *Engine
}

func newRedisDataDogHandler(withAnalytics bool, engine *Engine) *redisDataDogHandler {
	return &redisDataDogHandler{withAnalytics, engine}
}

func (h *redisDataDogHandler) HandleLog(e *apexLox.Entry) error {
	started := time.Unix(0, e.Fields.Get("started").(int64))
	operation := e.Fields.Get("operation").(string)
	span, _ := tracer.StartSpanFromContext(h.engine.dataDog.ctx[len(h.engine.dataDog.ctx)-1], operation, tracer.StartTime(started))
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
	return nil
}

type elasticDataDogHandler struct {
	withAnalytics bool
	engine        *Engine
}

func newElasticDataDogHandler(withAnalytics bool, engine *Engine) *elasticDataDogHandler {
	return &elasticDataDogHandler{withAnalytics, engine}
}

func (h *elasticDataDogHandler) HandleLog(e *apexLox.Entry) error {
	started := time.Unix(0, e.Fields.Get("started").(int64))
	span, _ := tracer.StartSpanFromContext(h.engine.dataDog.ctx[len(h.engine.dataDog.ctx)-1], "elasticsearch.query", tracer.StartTime(started))
	span.SetTag(ext.SpanType, ext.SpanTypeElasticSearch)
	span.SetTag(ext.ServiceName, "elasticsearch."+e.Fields.Get("pool").(string))
	span.SetTag(ext.ResourceName, e.Fields.Get("type").(string)+" "+e.Fields.Get("Index").(string))
	span.SetTag("elasticsearch.index", e.Fields.Get("Index"))
	span.SetTag("elasticsearch.type", e.Fields.Get("type"))
	span.SetTag("elasticsearch.params", e.Fields.Get("post"))
	span.SetTag("elasticsearch.page.from", e.Fields.Get("from"))
	span.SetTag("elasticsearch.page.size", e.Fields.Get("size"))

	if h.withAnalytics {
		span.SetTag(ext.AnalyticsEvent, true)
	}
	injectError(e, span)
	finished := time.Unix(0, e.Fields.Get("finished").(int64))
	span.Finish(tracer.FinishTime(finished))
	return nil
}

type clickHouseDataDogHandler struct {
	withAnalytics bool
	engine        *Engine
}

func newClickHouseDataDogHandler(withAnalytics bool, engine *Engine) *clickHouseDataDogHandler {
	return &clickHouseDataDogHandler{withAnalytics, engine}
}

func (h *clickHouseDataDogHandler) HandleLog(e *apexLox.Entry) error {
	started := time.Unix(0, e.Fields.Get("started").(int64))
	span, _ := tracer.StartSpanFromContext(h.engine.dataDog.ctx[len(h.engine.dataDog.ctx)-1], "clickhouse.query", tracer.StartTime(started))
	queryType := e.Fields.Get("type")
	span.SetTag(ext.SpanType, ext.SpanTypeSQL)
	span.SetTag(ext.ServiceName, "clickhouse."+e.Fields.Get("pool").(string))
	span.SetTag(ext.ResourceName, e.Fields.Get("Query"))
	span.SetTag(ext.SQLType, queryType)
	if h.withAnalytics {
		span.SetTag(ext.AnalyticsEvent, true)
	}
	injectError(e, span)
	finished := time.Unix(0, e.Fields.Get("finished").(int64))
	span.Finish(tracer.FinishTime(finished))
	return nil
}

func injectError(e *apexLox.Entry, span tracer.Span) {
	err := e.Fields.Get("error")
	if err != nil {
		span.SetTag(ext.Error, 1)
		span.SetTag(ext.ErrorMsg, err)
		span.SetTag(ext.ErrorDetails, strings.ReplaceAll(e.Fields.Get("stack_full").(string), "\\n", "\n"))
		span.SetTag(ext.ErrorStack, strings.ReplaceAll(e.Fields.Get("stack").(string), "\\n", "\n"))
		span.SetTag(ext.ErrorType, e.Fields.Get("error_type"))
	}
}

func injectLogError(err error, e *apexLox.Entry) *apexLox.Entry {
	stackParts := strings.Split(errors.ErrorStack(err), "\n")
	stack := strings.Join(stackParts[1:], "\\n")
	fullStack := strings.Join(clearStack(strings.Split(string(debug.Stack()), "\n")[4:]), "\\n")
	return e.WithError(err).
		WithField("stack", stack).
		WithField("stack_full", fullStack).
		WithField("error_type", reflect.TypeOf(errors.Cause(err)).String())
}
