package orm

import (
	"reflect"
	"runtime/debug"
	"strings"
	"time"

	log2 "github.com/apex/log"

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

func (e *Elastic) Search(index string, query elastic.Query, pager *Pager, callback func(*elastic.SearchService) (*elastic.SearchResult, error)) *elastic.SearchResult {
	start := time.Now()
	searchService := e.client.Search().Query(query)
	searchService.Index(index).From((pager.CurrentPage - 1) * pager.PageSize).Size(pager.PageSize).StoredField("_id")
	result, err := callback(searchService)
	if e.engine.queryLoggers[QueryLoggerSourceElastic] != nil {
		s, _ := query.Source()
		queryType := strings.Split(reflect.TypeOf(query).Elem().String(), ".")
		fields := log2.Fields{"Index": index, "post": s, "type": queryType[len(queryType)-1]}
		e.fillLogFields("[ORM][ELASTIC][QUERY]", start, "query", fields, err)
	}
	e.engine.dataDog.incrementCounter(counterElasticAll, 1)
	e.engine.dataDog.incrementCounter(counterElasticSearch, 1)
	if err != nil {
		panic(err)
	}
	return result
}

func (e *Elastic) fillLogFields(message string, start time.Time, operation string, fields log2.Fielder, err error) {
	now := time.Now()
	stop := time.Since(start).Microseconds()
	entry := e.engine.queryLoggers[QueryLoggerSourceRedis].log.
		WithField("microseconds", stop).
		WithField("operation", operation).
		WithField("pool", e.code).
		WithField("target", "elastic").
		WithField("started", start.UnixNano()).
		WithField("finished", now.UnixNano())
	if fields != nil {
		entry = entry.WithFields(fields)
	}
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
