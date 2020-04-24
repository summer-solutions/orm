package tests

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

type TestEntityMySQL struct {
	orm.ORM
	ID   uint
	Name string
}

func TestMySQL(t *testing.T) {
	var entity TestEntityMySQL
	engine := PrepareTables(t, &orm.Registry{}, entity, entity)
	for i := 1; i <= 5; i++ {
		e := &TestEntityMySQL{Name: "Name " + strconv.Itoa(i)}
		engine.RegisterEntity(e)
		err := e.Flush()
		assert.Nil(t, err)
	}

	db := engine.GetMysql()
	assert.NotNil(t, db)
}
