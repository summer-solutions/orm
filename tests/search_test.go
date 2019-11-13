package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"reflect"
	"strconv"
	"testing"
)

type TestEntitySearch struct {
	Orm            *orm.ORM `orm:"table=TestSearch;mysql=default"`
	Id             uint
	Name           string
	ReferenceOneId uint `orm:"ref=tests.TestEntitySearch"`
}

func TestSearch(t *testing.T) {

	PrepareTables(TestEntitySearch{})
	var entity TestEntitySearch

	var entities = make([]interface{}, 10)
	for i := 1; i <= 10; i++ {
		e := TestEntitySearch{Name: "Name " + strconv.Itoa(i)}
		if i >= 5 {
			e.ReferenceOneId = uint(i - 4)
		}
		entities[i-1] = &e
	}
	err := orm.Flush(entities...)
	assert.Nil(t, err)

	pager := orm.NewPager(1, 100)
	where := orm.NewWhere("`Id` > ? AND `Id` < ?", 1, 8)
	var rows []TestEntitySearch
	orm.Search(where, pager, &rows)
	assert.Len(t, rows, 6)
	assert.Equal(t, uint(2), rows[0].Id)
	assert.Equal(t, uint(7), rows[5].Id)

	pager = orm.NewPager(1, 4)

	rows = make([]TestEntitySearch, 0)
	totalRows := orm.SearchWithCount(where, pager, &rows)
	assert.Len(t, rows, 4)
	assert.Equal(t, 6, totalRows)
	assert.Equal(t, uint(2), rows[0].Id)
	assert.Equal(t, uint(5), rows[3].Id)

	pager = orm.NewPager(2, 4)
	rows = make([]TestEntitySearch, 0)
	totalRows = orm.SearchWithCount(where, pager, &rows)
	assert.Len(t, rows, 2)
	assert.Equal(t, 6, totalRows)
	assert.Equal(t, uint(6), rows[0].Id)
	assert.Equal(t, uint(7), rows[1].Id)

	pager = orm.NewPager(1, 6)
	rows = make([]TestEntitySearch, 0)
	totalRows = orm.SearchWithCount(where, pager, &rows)
	assert.Len(t, rows, 6)
	assert.Equal(t, 6, totalRows)
	assert.Equal(t, uint(2), rows[0].Id)
	assert.Equal(t, uint(7), rows[5].Id)

	ids := orm.SearchIds(where, pager, reflect.TypeOf(entity))
	assert.Len(t, ids, 6)
	assert.Equal(t, uint64(2), ids[0])
	assert.Equal(t, uint64(7), ids[5])
}
