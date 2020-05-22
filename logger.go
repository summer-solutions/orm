package orm

import (
	"github.com/apex/log"
	"github.com/apex/log/handlers/multi"
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
	entry := l.WithField("source", "orm")
	entry.Level = level
	return &logger{log: entry, handler: multiHandler}
}
