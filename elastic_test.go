package orm

import (
	"testing"

	"github.com/olivere/elastic/v7"

	"github.com/stretchr/testify/assert"
)

func TestElastic(t *testing.T) {
	t.SkipNow()
	registry := &Registry{}
	registry.RegisterElastic("http://127.0.0.1:9207")
	engine := PrepareTables(t, registry)
	e := engine.GetElastic()
	query := elastic.NewBoolQuery()
	query.Must(elastic.NewTermQuery("category", "test"))
	res := e.Search("kibana_sample_data_ecommerce", query, NewPager(1, 10), nil)
	assert.NotNil(t, res)
}
