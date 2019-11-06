package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"strconv"
	"testing"
)

const TestEntityFlushLazyName = "tests.TestEntityFlushLazy"

type TestEntityFlushLazy struct {
	Orm  orm.ORM `orm:"table=TestFlushLazy;mysql=default"`
	Id   uint
	Name string
}

func TestFlushLazy(t *testing.T) {
	var entity TestEntityFlushLazy
	PrepareTables(entity)

	Logger := TestDatabaseLogger{}
	orm.GetMysqlDB("default").AddLogger(&Logger)

	var entities = make([]interface{}, 10)
	for i := 1; i <= 10; i++ {
		e := TestEntityFlushLazy{Name: "Name " + strconv.Itoa(i)}
		entities[i-1] = &e
	}
	err := orm.FlushLazy(entities...)
	assert.Nil(t, err)
	assert.Len(t, Logger.Queries, 0)

	_, found := orm.TryById(1, TestEntityFlushLazyName)
	assert.False(t, found)
}
