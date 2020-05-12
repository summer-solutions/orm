package orm

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testEntityMySQL struct {
	ORM
	ID   uint
	Name string
}

func TestMySQL(t *testing.T) {
	var entity testEntityMySQL
	engine := PrepareTables(t, &Registry{}, entity, entity)
	defer engine.Defer()
	for i := 1; i <= 5; i++ {
		e := &testEntityMySQL{Name: "Name " + strconv.Itoa(i)}
		engine.Track(e)
	}
	err := engine.Flush()
	assert.Nil(t, err)

	db := engine.GetMysql()
	assert.NotNil(t, db)
}
