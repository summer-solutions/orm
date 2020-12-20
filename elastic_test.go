package orm

import (
	"testing"

	apexLog "github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/olivere/elastic/v7"

	"github.com/stretchr/testify/assert"
)

type TestIndex struct {
	Definition map[string]interface{}
}

func (i *TestIndex) GetName() string {
	return "test_index"
}

func (i *TestIndex) GetDefinition() map[string]interface{} {
	return i.Definition
}

func TestElastic(t *testing.T) {
	registry := &Registry{}
	registry.RegisterElastic("http://127.0.0.1:9209")
	index := &TestIndex{}
	index.Definition = map[string]interface{}{
		"settings": map[string]interface{}{
			"search": map[string]interface{}{
				"idle": map[string]interface{}{
					"after": "600s",
				},
			},
			"number_of_replicas": "1",
			"number_of_shards":   "1",
			"analysis": map[string]interface{}{
				"normalizer": map[string]interface{}{
					"case_insensitive": map[string]interface{}{
						"type":   "custom",
						"filter": []interface{}{"lowercase", "asciifolding"},
					},
				},
			},
		},
		"mappings": map[string]interface{}{
			"dynamic": "strict",
			"properties": map[string]interface{}{
				"Name": map[string]interface{}{
					"type":       "keyword",
					"normalizer": "case_insensitive",
				},
				"TestID": map[string]interface{}{
					"type": "keyword",
				},
			},
		},
	}

	registry.RegisterElasticIndex(index)
	engine := PrepareTables(t, registry, 5)

	testLogger := memory.New()
	engine.AddQueryLogger(testLogger, apexLog.InfoLevel, QueryLoggerSourceElastic)
	engine.DataDog().EnableORMAPMLog(apexLog.DebugLevel, true, QueryLoggerSourceElastic)

	e := engine.GetElastic()
	assert.NotNil(t, e.Client())
	query := elastic.NewBoolQuery()
	query.Must(elastic.NewTermQuery("category", "test"))
	options := SearchOptions{}
	options.AddSort("Name", true)
	res := e.Search("test_index", query, NewPager(1, 10), &options)
	assert.NotNil(t, res)

	engine.DataDog().StartWorkSpan("test")
	engine.DataDog().StartAPM("test_service", "test")
	engine.DataDog().StartWorkSpan("test")

	options = SearchOptions{}
	options.AddSort("Name", false)
	res = e.Search("test_index", query, NewPager(1, 10), &options)
	assert.NotNil(t, res)

	options = SearchOptions{}
	options.AddSort("Invalid", false)
	assert.Panics(t, func() {
		e.Search("test_index", query, NewPager(1, 10), &options)
	})

	options = SearchOptions{}
	sumAgg := elastic.NewCardinalityAggregation().Field("TestID")
	options.AddAggregation("TestID", sumAgg)
	res = e.Search("test_index", query, NewPager(1, 10), &options)
	assert.NotNil(t, res)

	alters := engine.GetElasticIndexAlters()
	assert.Len(t, alters, 0)
	e.DropIndex(index)
	alters = engine.GetElasticIndexAlters()
	assert.Len(t, alters, 1)
	assert.True(t, alters[0].Safe)
	e.CreateIndex(index)
	alters = engine.GetElasticIndexAlters()
	assert.Len(t, alters, 0)

	index.Definition["mappings"].(map[string]interface{})["properties"].(map[string]interface{})["LastName"] = map[string]interface{}{
		"type":       "keyword",
		"normalizer": "case_insensitive",
	}
	alters = engine.GetElasticIndexAlters()
	assert.Len(t, alters, 1)
}
