package orm

import (
	"reflect"
	"runtime/debug"
	"strings"

	apexLox "github.com/apex/log"

	"github.com/apex/log/handlers/multi"
)

type QueryLoggerSource int

const (
	QueryLoggerSourceDB = iota
	QueryLoggerSourceRedis
	QueryLoggerSourceElastic
	QueryLoggerSourceClickHouse
	QueryLoggerSourceLocalCache
	QueryLoggerSourceStreams
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

func injectLogError(err error, e *apexLox.Entry) *apexLox.Entry {
	stack := strings.Join(clearStack(strings.Split(string(debug.Stack()), "\n")[4:]), "\\n")
	return e.WithError(err).
		WithField("stack", stack).
		WithField("error_type", reflect.TypeOf(err).String())
}
