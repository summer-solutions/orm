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

	"github.com/segmentio/fasthash/fnv1a"

	apexLog "github.com/apex/log"

	"gopkg.in/DataDog/dd-trace-go.v1/profiler"

	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/ext"
	"gopkg.in/DataDog/dd-trace-go.v1/ddtrace/tracer"
)

type dataDog struct {
	engine   *Engine
	span     tracer.Span
	ctx      []context.Context
	apm      *apm
	hasError bool
}

type DataDog interface {
	StartAPM(service string, environment string)
	FinishAPM()
	StartHTTPAPM(request *http.Request, service string, environment string) HTTPAPM
	EnableORMAPMLog(level apexLog.Level, withAnalytics bool, source ...QueryLoggerSource)
	RegisterAPMError(err interface{})
	DropAPM()
	SetAPMTag(key string, value interface{})
	StartWorkSpan(name string) WorkSpan
}

type WorkSpan interface {
	Finish()
	SetTag(key string, value interface{})
}

type workSpan struct {
	span   tracer.Span
	engine *Engine
}

func (s *workSpan) Finish() {
	if s.span != nil {
		s.span.Finish()
		s.engine.dataDog.ctx = s.engine.dataDog.ctx[:len(s.engine.dataDog.ctx)-1]
	}
}

type workSpanEmpty struct {
}

func (s *workSpanEmpty) Finish() {
}

func (s *workSpanEmpty) SetTag(string, interface{}) {
}

func (s *workSpan) SetTag(key string, value interface{}) {
	if s.span != nil {
		s.span.SetTag(key, value)
	}
}

func (dd *dataDog) StartAPM(service string, environment string) {
	opts := []ddtrace.StartSpanOption{
		tracer.ServiceName(service),
		tracer.Measured(),
	}
	span, ctx := tracer.StartSpanFromContext(context.Background(), "service.run", opts...)
	span.SetTag(ext.AnalyticsEvent, true)
	span.SetTag(ext.Environment, environment)
	dd.engine.Log().AddFields(apexLog.Fields{"dd.trace_id": span.Context().TraceID(), "dd.span_id": span.Context().SpanID()})
	dd.span = span
	dd.ctx = []context.Context{ctx}
	dd.apm = &apm{engine: dd.engine}
}

func (dd *dataDog) FinishAPM() {
	if dd.apm != nil {
		dd.apm.finish()
	}
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
	s.engine.dataDog.span.Finish()
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

	if request.Method == http.MethodPost && request.PostForm != nil {
		_ = request.ParseForm()
		form := request.PostForm
		size := 0
		for key, v := range form {
			size += len(key)
			if len(v) > 0 {
				size += len(v[0])
			}
		}
		if size > 5120 {
			span.SetTag("post", "bigger than 5KiB")
		} else {
			postNice := make(map[string]string)
			for key, val := range form {
				if len(val) > 0 {
					postNice[key] = val[0]
				}
			}
			span.SetTag("post", postNice)
		}
	}
	span.SetTag(ext.Environment, environment)
	dd.engine.Log().AddFields(apexLog.Fields{"dd.trace_id": span.Context().TraceID(), "dd.span_id": span.Context().SpanID()})
	dd.span = span
	dd.ctx = []context.Context{ctx}
	return &httpAPM{apm{engine: dd.engine}, 0}
}

func (dd *dataDog) StartWorkSpan(name string) WorkSpan {
	l := len(dd.ctx)
	if l == 0 {
		return &workSpanEmpty{}
	}
	span, ctx := tracer.StartSpanFromContext(dd.ctx[len(dd.ctx)-1], name)
	dd.ctx = append(dd.ctx, ctx)
	span.SetTag(ext.AnalyticsEvent, false)
	return &workSpan{span, dd.engine}
}

func (dd *dataDog) EnableORMAPMLog(level apexLog.Level, withAnalytics bool, source ...QueryLoggerSource) {
	if len(source) == 0 {
		source = []QueryLoggerSource{QueryLoggerSourceDB, QueryLoggerSourceRedis, QueryLoggerSourceRabbitMQ, QueryLoggerSourceElastic,
			QueryLoggerSourceClickHouse, QueryLoggerSourceStreams}
	}
	for _, s := range source {
		if s == QueryLoggerSourceDB {
			dd.engine.AddQueryLogger(newDBDataDogHandler(withAnalytics, dd.engine), level, s)
		} else if s == QueryLoggerSourceRabbitMQ {
			dd.engine.AddQueryLogger(newRabbitMQDataDogHandler(withAnalytics, dd.engine), level, s)
		} else if s == QueryLoggerSourceRedis {
			dd.engine.AddQueryLogger(newRedisDataDogHandler(withAnalytics, dd.engine), level, s)
		} else if s == QueryLoggerSourceElastic {
			dd.engine.AddQueryLogger(newElasticDataDogHandler(withAnalytics, dd.engine), level, s)
		} else if s == QueryLoggerSourceClickHouse {
			dd.engine.AddQueryLogger(newClickHouseDataDogHandler(withAnalytics, dd.engine), level, s)
		} else if s == QueryLoggerSourceStreams {
			dd.engine.AddQueryLogger(newStreamsDataDogHandler(withAnalytics, dd.engine), level, s)
		}
	}
}

func (dd *dataDog) RegisterAPMError(err interface{}) {
	if dd.span != nil {
		asErr, ok := err.(error)
		if ok {
			dd.registerAPMError(asErr)
			return
		}
		lines := clearStack(strings.Split(string(debug.Stack()), "\n")[2:])
		fullStack := strings.Join(lines, "\n")
		hash := fnv1a.HashString32(fullStack)
		dd.span.SetTag(ext.Error, true)
		dd.span.SetTag(ext.ErrorMsg, fmt.Sprintf("%v", err))
		dd.span.SetTag(ext.ErrorStack, fullStack)
		dd.span.SetTag(ext.ErrorType, "panicRecovery")
		dd.span.SetTag("error.group", hash)
		dd.hasError = true
		dd.span.SetTag(ext.ManualKeep, true)
	}
}

func (dd *dataDog) DropAPM() {
	if dd.span != nil {
		dd.span.SetTag(ext.ManualDrop, true)
	}
}

func (dd *dataDog) SetAPMTag(key string, value interface{}) {
	if dd.span != nil {
		dd.span.SetTag(key, value)
	}
}

func StartDataDogTracer(rate float64, opts ...tracer.StartOption) (def func()) {
	opts = append(opts, tracer.WithAnalyticsRate(rate))
	tracer.Start(opts...)
	return func() { tracer.Stop() }
}

func StartDataDogProfiler(service string, apiKey string, environment string, duration time.Duration) (def func()) {
	_ = profiler.Start(
		//profiler.WithPeriod(duration),
		profiler.WithEnv(environment),
		profiler.WithAPIKey(apiKey),
		profiler.WithURL("https://intake.profile.datadoghq.eu/v1/input"),
		profiler.WithService(service),
	)
	return func() { profiler.Stop() }
}

func (dd *dataDog) registerAPMError(err error) {
	if dd.span != nil {
		lines := clearStack(strings.Split(string(debug.Stack()), "\n")[2:])
		stack := strings.Join(lines, "\n")
		hash := fnv1a.HashString32(stack)
		dd.span.SetTag(ext.Error, true)
		dd.span.SetTag(ext.ErrorMsg, err.Error())
		dd.span.SetTag(ext.ErrorStack, stack)
		dd.span.SetTag(ext.ErrorType, reflect.TypeOf(err).String())
		dd.span.SetTag("error.group", hash)
		dd.hasError = true
		dd.span.SetTag(ext.ManualKeep, true)
	}
}
