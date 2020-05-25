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
	engine   *Engine
	span     tracer.Span
	ctx      context.Context
	hasError bool
	counters map[string]uint
}

type DataDog interface {
	StartAPM(service string, environment string) APM
	StartHTTPAPM(request *http.Request, service string, environment string) HTTPAPM
	EnableORMAPMLog(level log.Level, withAnalytics bool, source ...LoggerSource)
	RegisterAPMError(err error)
	RegisterAPMRecovery(err interface{})
	DropAPM()
	SetAPMTag(key string, value interface{})
	StartWorkSpan(name string) WorkSpan
}

type WorkSpan interface {
	Finish()
	SetTag(key string, value interface{})
}

type workSpan struct {
	span tracer.Span
}

func (s *workSpan) Finish() {
	if s.span != nil {
		s.span.Finish()
	}
}

func (s *workSpan) SetTag(key string, value interface{}) {
	if s.span != nil {
		s.span.SetTag(key, value)
	}
}

func (dd *dataDog) StartAPM(service string, environment string) APM {
	opts := []ddtrace.StartSpanOption{
		tracer.ServiceName(service),
		tracer.Measured(),
	}
	span, ctx := tracer.StartSpanFromContext(context.Background(), "service.run", opts...)
	span.SetTag(ext.AnalyticsEvent, true)
	span.SetTag(ext.Environment, environment)
	dd.span = span
	dd.ctx = ctx
	return &apm{engine: dd.engine}
}

type APM interface {
	Finish()
}

type HTTPAPM interface {
	APM
	SetResponseStatus(status int)
}

type apm struct {
	engine *Engine
}

type httpAPM struct {
	apm
	status int
}

func (s *apm) finish() {
	dd := s.engine.dataDog
	for k, v := range dd.counters {
		if v > 0 {
			dd.span.SetTag("orm."+k, v)
			dd.counters[k] = 0
		}
	}
	s.engine.dataDog.span.Finish()
}

func (s *apm) Finish() {
	s.finish()
}

func (s *httpAPM) Finish() {
	dd := s.engine.dataDog
	dd.span.SetTag(ext.HTTPCode, strconv.Itoa(s.status))
	if s.status >= 500 && s.status < 600 {
		if !dd.hasError {
			dd.span.SetTag(ext.Error, fmt.Errorf("%d: %s", s.status, http.StatusText(s.status)))
		}
	}
	s.finish()
}

func (s *httpAPM) SetResponseStatus(status int) {
	s.status = status
}

func (dd *dataDog) StartHTTPAPM(request *http.Request, service string, environment string) HTTPAPM {
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
	return &httpAPM{apm{engine: dd.engine}, 0}
}

func (dd *dataDog) StartWorkSpan(name string) WorkSpan {
	if dd.span == nil {
		return &workSpan{}
	}
	span, _ := tracer.StartSpanFromContext(dd.ctx, name)
	return &workSpan{span}
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
		} else if s == LoggerSourceRabbitMQ {
			dd.engine.AddLogger(newRabbitMQDataDogHandler(dd.ctx, withAnalytics, dd.engine), level, s)
		} else if s == LoggerSourceRedis {
			dd.engine.AddLogger(newRedisDataDogHandler(dd.ctx, withAnalytics, dd.engine), level, s)
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

func (dd *dataDog) incrementCounter(key string, value uint) {
	before, has := dd.counters[key]
	if has {
		dd.counters[key] = before + value
	} else {
		if dd.counters == nil {
			dd.counters = make(map[string]uint)
		}
		dd.counters[key] = value
	}
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
