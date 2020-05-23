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
	started := time.Unix(e.Fields.Get("started_seconds").(int64), e.Fields.Get("started_nano").(int64))
	span, _ := tracer.StartSpanFromContext(h.ctx, "mysql.query", tracer.StartTime(started))
	span.SetTag(ext.SpanType, ext.SpanTypeSQL)
	span.SetTag(ext.ServiceName, "mysql.db."+e.Fields.Get("pool").(string))
	span.SetTag(ext.SQLType, e.Fields.Get("type"))
	span.SetTag(ext.SQLQuery, e.Fields.Get("Query"))
	span.SetTag(ext.ResourceName, e.Fields.Get("Query"))
	span.SetTag(ext.DBName, e.Fields.Get("db"))
	err := e.Fields.Get("error")
	if err != nil {
		span.SetTag(ext.ErrorMsg, err)
		span.SetTag(ext.ErrorStack, strings.ReplaceAll(e.Fields.Get("stack").(string), "\\n", "\n"))
		span.SetTag(ext.ErrorType, e.Fields.Get("error_type"))
	}
	span.SetTag(ext.AnalyticsEvent, true)
	finished := time.Unix(e.Fields.Get("finished_seconds").(int64), e.Fields.Get("finished_nano").(int64))
	span.Finish(tracer.FinishTime(finished))
	return nil
}
