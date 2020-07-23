package orm

import (
	"testing"

	"github.com/olivere/elastic/v7"

	"github.com/stretchr/testify/assert"
)

type TestIndex struct {
}

func (i *TestIndex) GetName() string {
	return "test_index"
}

func (i *TestIndex) GetDefinition() map[string]interface{} {
	return map[string]interface{}{
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
			},
		},
	}
}

func TestElastic(t *testing.T) {
	registry := &Registry{}
	registry.RegisterElastic("http://127.0.0.1:9207")
	registry.RegisterElasticIndex(&TestIndex{})
	engine := PrepareTables(t, registry)
	e := engine.GetElastic()
	query := elastic.NewBoolQuery()
	query.Must(elastic.NewTermQuery("category", "test"))
	res := e.Search("test_index", query, NewPager(1, 10), nil)
	assert.NotNil(t, res)
}
