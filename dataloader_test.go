package orm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type dataLoaderEntity struct {
	ORM  `orm:"redisCache"`
	ID   uint
	Name string `orm:"max=100"`
}

func TestDataLoader(t *testing.T) {
	var entity *dataLoaderEntity
	engine := PrepareTables(t, &Registry{}, entity)
	engine.EnableDataLoader(100, time.Millisecond)

	engine.TrackAndFlush(&dataLoaderEntity{Name: "a"})

	entity = &dataLoaderEntity{}
	found := engine.LoadByID(1, entity)
	assert.True(t, found)
	assert.Equal(t, uint(1), entity.ID)
	assert.Equal(t, "a", entity.Name)
}
