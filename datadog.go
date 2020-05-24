package orm

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"runtime/debug"
	"strconv"
	"strings"

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
}

type DataDog interface {
	StartHTTPAPM(request *http.Request, service string) (tracer.Span, context.Context)
	StopHTTPAPM(status int)
	EnableORMAPMLog(level log.Level, source ...LoggerSource)
	RegisterAPMError(err error)
	RegisterAPMRecovery(err interface{}, skipLines int)
}

func (dd *dataDog) StartHTTPAPM(request *http.Request, service string) (tracer.Span, context.Context) {
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
}

func (dd *dataDog) EnableORMAPMLog(level log.Level, source ...LoggerSource) {
	if dd.span == nil {
		return
	}
	if len(source) == 0 {
		source = []LoggerSource{LoggerSourceDB, LoggerSourceRedis, LoggerSourceRabbitMQ}
	}
	for _, s := range source {
		if s == LoggerSourceDB {
			dd.engine.AddLogger(newDBDataDogHandler(dd.ctx), level, s)
		} else if s == LoggerSourceRabbitMQ {
			dd.engine.AddLogger(newRabbitMQDataDogHandler(dd.ctx), level, s)
		} else if s == LoggerSourceRedis {
			dd.engine.AddLogger(newRedisDataDogHandler(dd.ctx), level, s)
		}
	}
}

func (dd *dataDog) RegisterAPMError(err error) {
	if dd.span == nil {
		return
	}
	stackParts := strings.Split(errors.ErrorStack(err), "\n")
	stack := strings.Join(stackParts[1:], "\n")
	fullStack := strings.Join(strings.Split(string(debug.Stack()), "\n")[2:], "\n")
	dd.span.SetTag(ext.Error, true)
	dd.span.SetTag(ext.ErrorMsg, err.Error())
	dd.span.SetTag(ext.ErrorDetails, fullStack)
	dd.span.SetTag(ext.ErrorStack, stack)
	dd.span.SetTag(ext.ErrorType, reflect.TypeOf(errors.Cause(err)).String())
	dd.hasError = true
}

func (dd *dataDog) RegisterAPMRecovery(err interface{}, skipLines int) {
	if dd.span == nil {
		return
	}
	asErr, ok := err.(error)
	if ok {
		dd.RegisterAPMError(asErr)
		return
	}
	lines := strings.Split(string(debug.Stack()), "\n")[skipLines:]
	fullStack := strings.Join(lines, "\n")
	source := strings.Split(lines[0], " ")[0]
	dd.span.SetTag(ext.Error, true)
	dd.span.SetTag("error.source", source)
	dd.span.SetTag(ext.ErrorMsg, fmt.Sprintf("%v", err))
	dd.span.SetTag(ext.ErrorStack, fullStack)
	dd.span.SetTag(ext.ErrorType, "panicRecovery")
	dd.hasError = true
}
