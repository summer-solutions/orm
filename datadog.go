package orm

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"gopkg.in/DataDog/dd-trace-go.v1/profiler"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"

	"github.com/juju/errors"

	"github.com/apex/log"
)

type dataDog struct {
	engine                 *Engine
	span                   tracer.Span
	ctx                    context.Context
	hasError               bool
	logDB                  bool
	logRedis               bool
	logRabbitMQ            bool
	dbExecs                uint
	dbTransactions         uint
	dbQueries              uint
	dbAll                  uint
	redisAll               uint
	redisKeys              uint
	redisMisses            uint
	redisSets              uint
	redisSetsKeys          uint
	redisDeletes           uint
	redisDeletesKeys       uint
	redisGets              uint
	redisGetsKeys          uint
	rabbitMQAll            uint
	rabbitMQCloseChannels  uint
	rabbitMQCreateChannels uint
	rabbitMQConsumes       uint
	rabbitMQACKs           uint
	rabbitMQReceivers      uint
	rabbitMQPublished      uint
	rabbitMQConnects       uint
	rabbitMQRegisters      uint
	lockerAll              uint
}

type DataDog interface {
	StartHTTPAPM(request *http.Request, service string, environment string) (tracer.Span, context.Context)
	StopHTTPAPM(status int)
	EnableORMAPMLog(level log.Level, withAnalytics bool, source ...LoggerSource)
	RegisterAPMError(err error)
	RegisterAPMRecovery(err interface{})
	DropAPM()
	SetAPMTag(key string, value interface{})
}

func (dd *dataDog) StartHTTPAPM(request *http.Request, service string, environment string) (tracer.Span, context.Context) {
	resource := request.Method + " " + request.URL.Path
	opts := []ddtrace.StartSpanOption{
		tracer.ServiceName(service),
		tracer.ResourceName(resource),
		tracer.SpanType(ext.SpanTypeWeb),
		tracer.Tag(ext.HTTPMethod, request.Method),
		tracer.Tag(ext.HTTPURL, request.URL.Path),
		tracer.Measured(),
	}
	if spanCtx, err := tracer.Extract(tracer.HTTPHeadersCarrier(request.Header)); err == nil {
		opts = append(opts, tracer.ChildOf(spanCtx))
	}
	span, ctx := tracer.StartSpanFromContext(request.Context(), "http.request", opts...)
	span.SetTag(ext.AnalyticsEvent, true)
	q := request.URL.Query()
	if len(q) > 0 {
		span.SetTag("url.query", request.URL.RawQuery)
	}
	span.SetTag(ext.Environment, environment)
	dd.span = span
	dd.ctx = ctx
	return span, ctx
}

func (dd *dataDog) StopHTTPAPM(status int) {
	if dd.span == nil {
		return
	}
	dd.span.SetTag(ext.HTTPCode, strconv.Itoa(status))
	if status >= 500 && status < 600 {
		if !dd.hasError {
			dd.span.SetTag(ext.Error, fmt.Errorf("%d: %s", status, http.StatusText(status)))
		}
	}
	if dd.logDB {
		dd.span.SetTag("orm.db.all", dd.dbAll)
		dd.span.SetTag("orm.db.exec", dd.dbExecs)
		dd.span.SetTag("orm.db.query", dd.dbQueries)
		dd.span.SetTag("orm.db.transaction", dd.dbTransactions)
		dd.dbAll = 0
		dd.dbExecs = 0
		dd.dbQueries = 0
		dd.dbTransactions = 0
	}
	if dd.logRedis {
		dd.span.SetTag("orm.redis.all", dd.redisAll)
		dd.span.SetTag("orm.redis.keys", dd.redisKeys)
		dd.span.SetTag("orm.redis.misses", dd.redisMisses)
		dd.span.SetTag("orm.redis.sets", dd.redisSets)
		dd.span.SetTag("orm.redis.setKeys", dd.redisSetsKeys)
		dd.span.SetTag("orm.redis.gets", dd.redisGets)
		dd.span.SetTag("orm.redis.getKeys", dd.redisGetsKeys)
		dd.span.SetTag("orm.redis.deletes", dd.redisDeletes)
		dd.span.SetTag("orm.redis.deleteKeys", dd.redisDeletesKeys)
		dd.span.SetTag("orm.redis.locker", dd.lockerAll)
		dd.redisAll = 0
		dd.redisKeys = 0
		dd.redisMisses = 0
		dd.redisSets = 0
		dd.redisSetsKeys = 0
		dd.redisGets = 0
		dd.redisGetsKeys = 0
		dd.redisDeletes = 0
		dd.redisDeletesKeys = 0
		dd.lockerAll = 0
	}
	if dd.logRabbitMQ {
		dd.span.SetTag("orm.rabbitMQ.all", dd.rabbitMQAll)
		dd.span.SetTag("orm.rabbitMQ.close_channels", dd.rabbitMQCloseChannels)
		dd.span.SetTag("orm.rabbitMQ.create_channels", dd.rabbitMQCreateChannels)
		dd.span.SetTag("orm.rabbitMQ.consumes", dd.rabbitMQConsumes)
		dd.span.SetTag("orm.rabbitMQ.acks", dd.rabbitMQACKs)
		dd.span.SetTag("orm.rabbitMQ.receives", dd.rabbitMQReceivers)
		dd.span.SetTag("orm.rabbitMQ.published", dd.rabbitMQPublished)
		dd.span.SetTag("orm.rabbitMQ.connects", dd.rabbitMQConnects)
		dd.span.SetTag("orm.rabbitMQ.registers", dd.rabbitMQRegisters)
		dd.rabbitMQAll = 0
		dd.rabbitMQCloseChannels = 0
		dd.rabbitMQCreateChannels = 0
		dd.rabbitMQConsumes = 0
		dd.rabbitMQACKs = 0
		dd.rabbitMQReceivers = 0
		dd.rabbitMQPublished = 0
		dd.rabbitMQConnects = 0
		dd.rabbitMQRegisters = 0
	}
}

func (dd *dataDog) EnableORMAPMLog(level log.Level, withAnalytics bool, source ...LoggerSource) {
	if dd.span == nil {
		return
	}
	if len(source) == 0 {
		source = []LoggerSource{LoggerSourceDB, LoggerSourceRedis, LoggerSourceRabbitMQ}
	}
	for _, s := range source {
		if s == LoggerSourceDB {
			dd.engine.AddLogger(newDBDataDogHandler(dd.ctx, withAnalytics, dd.engine), level, s)
			if level >= log.InfoLevel {
				dd.logDB = true
			}
		} else if s == LoggerSourceRabbitMQ {
			if level >= log.InfoLevel {
				dd.logRabbitMQ = true
			}
			dd.engine.AddLogger(newRabbitMQDataDogHandler(dd.ctx, withAnalytics, dd.engine), level, s)
		} else if s == LoggerSourceRedis {
			dd.engine.AddLogger(newRedisDataDogHandler(dd.ctx, withAnalytics, dd.engine), level, s)
			if level >= log.InfoLevel {
				dd.logRedis = true
			}
		}
	}
}

func (dd *dataDog) RegisterAPMError(err error) {
	if dd.span == nil {
		return
	}
	stackParts := strings.Split(errors.ErrorStack(err), "\n")
	details := strings.Join(stackParts[1:], "\n")
	lines := strings.Split(string(debug.Stack()), "\n")[2:]
	fullStack := strings.Join(lines, "\n")
	dd.span.SetTag(ext.Error, true)
	dd.span.SetTag(ext.ErrorMsg, err.Error())
	dd.span.SetTag(ext.ErrorDetails, details)
	dd.span.SetTag(ext.ErrorStack, fullStack)
	dd.span.SetTag(ext.ErrorType, reflect.TypeOf(errors.Cause(err)).String())
	dd.hasError = true
	dd.span.SetTag(ext.ManualKeep, true)
}

func (dd *dataDog) RegisterAPMRecovery(err interface{}) {
	if dd.span == nil {
		return
	}
	asErr, ok := err.(error)
	if ok {
		dd.RegisterAPMError(asErr)
		return
	}
	lines := strings.Split(string(debug.Stack()), "\n")[2:]
	fullStack := strings.Join(lines, "\n")
	dd.span.SetTag(ext.Error, true)
	dd.span.SetTag(ext.ErrorMsg, fmt.Sprintf("%v", err))
	dd.span.SetTag(ext.ErrorStack, fullStack)
	dd.span.SetTag(ext.ErrorType, "panicRecovery")
	dd.hasError = true
	dd.span.SetTag(ext.ManualKeep, true)
}

func (dd *dataDog) DropAPM() {
	if dd.span == nil {
		return
	}
	dd.span.SetTag(ext.ManualDrop, true)
}

func (dd *dataDog) SetAPMTag(key string, value interface{}) {
	if dd.span == nil {
		return
	}
	dd.span.SetTag(key, value)
}

func StartDataDogTracer(rate float64) (def func()) {
	tracer.Start(tracer.WithAnalyticsRate(rate))
	return func() { tracer.Stop() }
}

func StartDataDogProfiler(service string, apiKey string, environment string, duration time.Duration) (def func()) {
	_ = profiler.Start(
		profiler.WithPeriod(duration),
		profiler.WithEnv(environment),
		profiler.WithAPIKey(apiKey),
		profiler.WithURL("https://intake.profile.datadoghq.eu/v1/input"),
		profiler.WithService(service),
	)
	return func() { profiler.Stop() }
}
