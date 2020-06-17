package orm

import (
	"context"
	"reflect"
	"strings"
	"time"

	log2 "github.com/apex/log"

	"github.com/olivere/elastic/v7"
)

const counterElasticAll = "elastic.all"
const counterElasticSearch = "elastic.search"

type ElasticSort struct {
	fields []string
	asc    []bool
}

func (s *ElasticSort) Add(field string, ascending bool) *ElasticSort {
	if s.fields == nil {
		s.fields = make([]string, 0)
		s.asc = make([]bool, 0)
	}
	s.fields = append(s.fields, field)
	s.asc = append(s.asc, ascending)
	return s
}

type Elastic struct {
	engine *Engine
	code   string
	client *elastic.Client
}

func (e *Elastic) Client() *elastic.Client {
	return e.client
}

func (e *Elastic) Search(index string, query elastic.Query, pager *Pager, sort *ElasticSort) *elastic.SearchResult {
	start := time.Now()
	searchService := e.client.Search().Query(query)
	from := (pager.CurrentPage - 1) * pager.PageSize
	searchService.Index(index).From(from).Size(pager.PageSize).StoredField("_id")
	if sort != nil {
		for i, v := range sort.fields {
			searchService.Sort(v, sort.asc[i])
		}
	}
	result, err := searchService.Do(context.Background())
	if e.engine.queryLoggers[QueryLoggerSourceElastic] != nil {
		s, _ := query.Source()
		queryType := strings.Split(reflect.TypeOf(query).Elem().String(), ".")
		fields := log2.Fields{"Index": index, "post": s, "type": queryType[len(queryType)-1], "from": from, "size": pager.PageSize}
		if sort != nil {
			sortFields := make([]string, len(sort.fields))
			for i, v := range sort.fields {
				asc := "ASC"
				if !sort.asc[i] {
					asc = "DESC"
				}
				sortFields[i] = v + " " + asc
				searchService.Sort(v, sort.asc[i])
			}
			fields["sort"] = sortFields
		}
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
	entry := e.engine.queryLoggers[QueryLoggerSourceElastic].log.
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
