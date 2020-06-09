package orm

import (
	"context"
	"testing"

	"github.com/olivere/elastic/v7"

	"github.com/stretchr/testify/assert"
)

func TestElastic(t *testing.T) {
	t.SkipNow()
	registry := &Registry{}
	registry.RegisterRabbitMQServer("amqp://rabbitmq_user:rabbitmq_password@localhost:5677/test")
	registry.RegisterElastic("http://127.0.0.1:9207")
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()
	e := engine.GetElastic()

	query := elastic.NewBoolQuery()
	query.Must(elastic.NewTermQuery("ZoneID", 12))
	e.Search("kibana_sample_data_ecommerce", query, &Pager{CurrentPage: 1, PageSize: 10}, func(searchService *elastic.SearchService) (*elastic.SearchResult, error) {
		return searchService.Do(context.Background())
	})
}
