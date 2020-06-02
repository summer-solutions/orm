package orm

import (
	"os"

	apexLog "github.com/apex/log"
	"github.com/apex/log/handlers/multi"
	jsoniter "github.com/json-iterator/go"
)

type log struct {
	engine *Engine
	logger *logger
}

type Log interface {
	AddFields(fields apexLog.Fielder)
	Debug(message string, fields apexLog.Fielder)
	Info(message string, fields apexLog.Fielder)
	Warn(message string, fields apexLog.Fielder)
	Error(message string, err error, fields apexLog.Fielder)
	Fatal(message string, err error, fields apexLog.Fielder)
}

func newLog(engine *Engine) *log {
	multiHandler := multi.New()
	l := &apexLog.Logger{Handler: multiHandler, Level: apexLog.DebugLevel}
	entry := apexLog.NewEntry(l)
	return &log{engine, &logger{log: entry, handler: multiHandler}}
}

func (l *log) AddFields(fields apexLog.Fielder) {
	l.logger.log = l.logger.log.WithFields(fields)
}

func (l *log) Debug(message string, fields apexLog.Fielder) {
	if fields != nil {
		l.logger.log.WithFields(fields).Debug(message)
		return
	}
	l.logger.log.Debug(message)
}

func (l *log) Info(message string, fields apexLog.Fielder) {
	if fields != nil {
		l.logger.log.WithFields(fields).Info(message)
		return
	}
	l.logger.log.Info(message)
}

func (l *log) Warn(message string, fields apexLog.Fielder) {
	if fields != nil {
		l.logger.log.WithFields(fields).Warn(message)
		return
	}
	l.logger.log.Warn(message)
}

func (l *log) Error(message string, err error, fields apexLog.Fielder) {
	if fields != nil {
		l.logger.log.WithFields(fields).WithError(err).Error(message)
		return
	}
	l.logger.log.WithError(err).Error(message)
}

func (l *log) Fatal(message string, err error, fields apexLog.Fielder) {
	if fields != nil {
		l.logger.log.WithFields(fields).WithError(err).Fatal(message)
		return
	}
	l.logger.log.WithError(err).Fatal(message)
}

type jsonHandler struct{}

func (h *jsonHandler) HandleLog(e *apexLog.Entry) error {
	fields := e.Fields
	fields["level"] = e.Level
	fields["timestamp"] = e.Timestamp
	fields["message"] = e.Message
	b, err := jsoniter.ConfigFastest.Marshal(fields)
	if err != nil {
		return err
	}
	_, _ = os.Stderr.Write(b)
	return nil
}
