package orm

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

type searchEntity struct {
	ORM          `orm:"localCache;redisCache"`
	ID           uint
	Name         string
	ReferenceOne *searchEntityReference
}

type searchEntityReference struct {
	ORM
	ID   uint
	Name string
}

func TestSearch(t *testing.T) {
	var entity *searchEntity
	var reference *searchEntityReference
	registry := &Registry{}
	engine := PrepareTables(t, registry, entity, reference)

	for i := 1; i < 10; i++ {
		engine.Track(&searchEntity{Name: fmt.Sprintf("name %d", i), ReferenceOne: &searchEntityReference{Name: fmt.Sprintf("name %d", i)}})
	}
	engine.Flush()

	var rows []*searchEntity
	missing := engine.LoadByIDs([]uint64{1, 2, 20}, &rows)
	assert.Len(t, missing, 1)
	assert.Len(t, rows, 2)
	assert.Equal(t, uint64(20), missing[0])
	assert.Equal(t, uint64(1), rows[0].ID)
	assert.Equal(t, uint64(2), rows[1].ID)
}
