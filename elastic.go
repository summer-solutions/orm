package orm

import (
	"context"
	"reflect"
	"strings"
	"time"

	"github.com/google/go-cmp/cmp"

	log2 "github.com/apex/log"

	"github.com/olivere/elastic/v7"
)

const counterElasticAll = "elastic.all"
const counterElasticSearch = "elastic.search"

type ElasticIndexDefinition interface {
	GetName() string
	GetDefinition() map[string]interface{}
}

type ElasticIndexAlter struct {
	Index ElasticIndexDefinition
	Safe  bool
	Pool  string
}

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
		if result != nil {
			fields["query_time"] = result.TookInMillis * 1000
		}
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
	checkError(err)
	return result
}

func (e *Elastic) DropIndex(index ElasticIndexDefinition) {
	ctx := context.Background()

	existService := elastic.NewIndicesExistsService(e.client)
	existService.Index([]string{index.GetName()})
	indexExists, err := existService.Do(ctx)
	checkError(err)
	if indexExists {
		_, err := e.client.DeleteIndex(index.GetName()).Do(ctx)
		checkError(err)
	}
}

func (e *Elastic) CreateIndex(index ElasticIndexDefinition) {
	ctx := context.Background()
	e.DropIndex(index)
	_, err := e.client.CreateIndex(index.GetName()).BodyJson(index.GetDefinition()).Do(ctx)
	checkError(err)
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

func getElasticIndexAlters(engine *Engine) (alters []ElasticIndexAlter) {
	alters = make([]ElasticIndexAlter, 0)
	if engine.registry.registry.elasticIndices != nil {
		ctx := context.Background()
		for pool, indices := range engine.registry.registry.elasticIndices {
			existService := elastic.NewIndicesExistsService(engine.GetElastic(pool).client)
			for name, index := range indices {
				existService.Index([]string{name})
				indexExists, err := existService.Do(ctx)
				checkError(err)
				if !indexExists {
					alters = append(alters, ElasticIndexAlter{Index: index, Safe: true, Pool: pool})
					continue
				}
				getMappingService := elastic.NewGetMappingService(engine.GetElastic(pool).client)
				getMappingService.Index(name)
				currentMapping, err := getMappingService.Do(ctx)
				checkError(err)

				currentMappingIndex := currentMapping[name].(map[string]interface{})
				getIndexSettingService := elastic.NewIndicesGetSettingsService(engine.GetElastic(pool).client)
				getIndexSettingService.Index(name)
				currentSettings, err := getIndexSettingService.Do(ctx)
				checkError(err)

				delete(currentSettings[name].Settings["index"].(map[string]interface{}), "creation_date")
				delete(currentSettings[name].Settings["index"].(map[string]interface{}), "provided_name")
				delete(currentSettings[name].Settings["index"].(map[string]interface{}), "uuid")
				delete(currentSettings[name].Settings["index"].(map[string]interface{}), "version")
				definition := index.GetDefinition()
				if !cmp.Equal(definition["mappings"], currentMappingIndex["mappings"]) ||
					!cmp.Equal(definition["settings"], currentSettings[name].Settings["index"]) {
					alters = append(alters, ElasticIndexAlter{Index: index, Safe: false, Pool: pool})
				}
			}
		}
	}
	return alters
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}
