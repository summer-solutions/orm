package orm

import (
	"fmt"
	"net/http"
	"os"
	"reflect"
	"runtime/debug"
	"strings"
	"time"

	"github.com/segmentio/fasthash/fnv1a"

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
	SetHTTPResponseCode(code int)
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
	fields["http.url_details.path"] = r.URL.Path
	fields["http.useragent"] = r.UserAgent()
	fields["http.method"] = r.Method
	fields["http.host"] = r.Host
	fields["http.query"] = r.URL.Query()
	fields["network.client.ip"] = ip
	l.AddFields(fields)
}

func (l *log) SetHTTPResponseCode(code int) {
	l.AddFields(apexLog.Fields{"http.status_code": code})
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
	lines := clearStack(strings.Split(string(debug.Stack()), "\n")[5:])
	stack := strings.Join(lines, "\n")
	hash := fnv1a.HashString32(stack)
	asErr, ok := err.(error)
	if ok {
		errorFields := apexLog.Fields{"error.message": asErr.Error()}
		errorFields["error.group"] = hash
		errorFields["error.stack"] = stack
		errorFields["error.kind"] = reflect.TypeOf(asErr).String()
		log = log.WithFields(errorFields)
		log.Error(asErr.Error())
		return
	}
	message := fmt.Sprintf("%v", err)
	errorFields := apexLog.Fields{"error.message": message}
	errorFields["error.group"] = hash
	errorFields["error.stack"] = stack
	errorFields["error.kind"] = "panicRecovery"
	log.WithFields(errorFields).Error(message)
}

type jsonHandler struct{}

func clearStack(stack []string) []string {
	cleared := make([]string, len(stack))
	for i, line := range stack {
		pos := strings.Index(line, "(0x")
		if pos > -1 {
			line = line[0:pos]
		} else {
			pos = strings.Index(line, " +0x")
			if pos > -1 {
				line = line[0:pos]
			}
		}
		cleared[i] = line
	}
	return cleared
}

func (h *jsonHandler) HandleLog(e *apexLog.Entry) error {
	fields := e.Fields
	fields["level"] = e.Level
	fields["timestamp"] = e.Timestamp
	fields["message"] = e.Message
	b, _ := jsoniter.ConfigFastest.MarshalToString(fields)
	_, _ = os.Stderr.WriteString(b + "\n")
	return nil
}
