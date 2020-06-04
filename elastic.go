package orm

import (
	"reflect"
	"runtime/debug"
	"strings"
	"time"

	"github.com/olivere/elastic/v7"

	"github.com/juju/errors"
)

const counterElasticAll = "elastic.all"
const counterElasticSearch = "elastic.search"

type Elastic struct {
	engine *Engine
	code   string
	client *elastic.Client
}

func (e *Elastic) Client() *elastic.Client {
	return e.client
}

func (e *Elastic) Search(callback func(*elastic.SearchService) *elastic.SearchResult, query elastic.Query) *elastic.SearchResult {
	start := time.Now()
	searchService := e.client.Search().Query(query)
	result := callback(searchService)
	if e.engine.queryLoggers[QueryLoggerSourceElastic] != nil {
		e.fillLogFields("[ORM][ELASTIC][QUERY]", start, "query", nil)
	}
	e.engine.dataDog.incrementCounter(counterElasticAll, 1)
	e.engine.dataDog.incrementCounter(counterElasticSearch, 1)
	return result
}

func (e *Elastic) fillLogFields(message string, start time.Time, operation string, err error) {
	now := time.Now()
	stop := time.Since(start).Microseconds()
	entry := e.engine.queryLoggers[QueryLoggerSourceRedis].log.
		WithField("microseconds", stop).
		WithField("operation", operation).
		WithField("pool", e.code).
		WithField("target", "elastic").
		WithField("started", start.UnixNano()).
		WithField("finished", now.UnixNano())
	if err != nil {
		stackParts := strings.Split(errors.ErrorStack(err), "\n")
		stack := strings.Join(stackParts[1:], "\\n")
		fullStack := strings.Join(strings.Split(string(debug.Stack()), "\n")[4:], "\\n")
		entry.WithError(err).
			WithField("stack", stack).
			WithField("stack_full", fullStack).
			WithField("error_type", reflect.TypeOf(errors.Cause(err)).String()).
			Error(message)
	} else {
		entry.Info(message)
	}
}
