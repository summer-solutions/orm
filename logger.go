package orm

import (
	"fmt"
	"net/http"
	"os"
	"reflect"
	"runtime/debug"
	"strings"
	"time"

	"github.com/juju/errors"

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
	Error(err interface{}, fields apexLog.Fielder)
	ErrorMessage(message string, fields apexLog.Fielder)
	AddFieldsFromHTTPRequest(r *http.Request, ip string)
}

func newLog(engine *Engine) *log {
	multiHandler := multi.New()
	l := &apexLog.Logger{Handler: multiHandler, Level: apexLog.DebugLevel}
	entry := apexLog.NewEntry(l).WithFields(apexLog.Fields{"logger.name": "github.com/summer-solutions/orm", "logger.thread_name": time.Now().UnixNano()})
	return &log{engine, &logger{log: entry, handler: multiHandler}}
}

func (l *log) AddFields(fields apexLog.Fielder) {
	l.logger.log = l.logger.log.WithFields(fields)
}

func (l *log) AddFieldsFromHTTPRequest(r *http.Request, ip string) {
	fields := apexLog.Fields{"http.url": r.RequestURI}
	fields["http.path"] = r.URL.Path
	fields["http.useragent"] = r.UserAgent()
	fields["http.method"] = r.Method
	fields["http.host"] = r.Host
	fields["http.query"] = r.URL.Query()
	fields["network.client.ip"] = ip
	l.AddFields(fields)
}

func (l *log) Debug(message string, fields apexLog.Fielder) {
	log := l.logger.log
	if fields != nil {
		log = log.WithFields(fields)
	}
	log.Debug(message)
}

func (l *log) Info(message string, fields apexLog.Fielder) {
	log := l.logger.log
	if fields != nil {
		log = log.WithFields(fields)
	}
	log.Info(message)
}

func (l *log) Warn(message string, fields apexLog.Fielder) {
	log := l.logger.log
	if fields != nil {
		log = log.WithFields(fields)
	}
	log.Warn(message)
}

func (l *log) ErrorMessage(message string, fields apexLog.Fielder) {
	log := l.logger.log
	if fields != nil {
		log = log.WithFields(fields)
	}
	log.Error(message)
}

func (l *log) Error(err interface{}, fields apexLog.Fielder) {
	log := l.logger.log
	if fields != nil {
		log = log.WithFields(fields)
	}
	lines := strings.Split(string(debug.Stack()), "\n")[2:]
	fullStack := strings.Join(lines, "\n")
	asErr, ok := err.(error)
	if ok {
		stackParts := strings.Split(errors.ErrorStack(asErr), "\n")
		details := strings.Join(stackParts[1:], "\n")
		errorFields := apexLog.Fields{"error.message": asErr.Error()}
		errorFields["error.stack"] = fullStack
		errorFields["error.details"] = details
		errorFields["error.kind"] = reflect.TypeOf(errors.Cause(asErr)).String()
		log = log.WithFields(errorFields)
		log.Error(asErr.Error())
		return
	}
	message := fmt.Sprintf("%v", err)
	errorFields := apexLog.Fields{"error.message": message}
	errorFields["error.stack"] = fullStack
	errorFields["error.kind"] = "panicRecovery"
	log.Error(message)
}

type jsonHandler struct{}

func (h *jsonHandler) HandleLog(e *apexLog.Entry) error {
	fields := e.Fields
	fields["level"] = e.Level
	fields["timestamp"] = e.Timestamp
	fields["message"] = e.Message
	b, err := jsoniter.ConfigFastest.MarshalToString(fields)
	if err != nil {
		return err
	}
	_, _ = os.Stderr.WriteString(b + "\n")
	return nil
}
