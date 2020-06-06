package orm

import (
	"reflect"
	"strings"
	"time"

	log2 "github.com/apex/log"

	"github.com/olivere/elastic/v7"
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
	from := (pager.CurrentPage - 1) * pager.PageSize
	searchService.Index(index).From(from).Size(pager.PageSize).StoredField("_id")
	result, err := callback(searchService)
	if e.engine.queryLoggers[QueryLoggerSourceElastic] != nil {
		s, _ := query.Source()
		queryType := strings.Split(reflect.TypeOf(query).Elem().String(), ".")
		fields := log2.Fields{"Index": index, "post": s, "type": queryType[len(queryType)-1], "from": from, "size": pager.PageSize}
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
		injectLogError(err, entry).Error(message)
	} else {
		entry.Info(message)
	}
}
