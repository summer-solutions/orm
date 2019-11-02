package tests

import (
	"github.com/stretchr/testify/assert"
	"orm"
	"strconv"
	"testing"
)

const TestEntitySearchName = "tests.TestEntitySearch"

type TestEntitySearch struct {
	Orm  orm.ORM `orm:"table=TestSearch;mysql=default"`
	Id   uint
	Name string
}

func TestSearch(t *testing.T) {
	PrepareTables(TestEntitySearch{})
	var entities = make([]interface{}, 10)
	for i := 1; i <= 10; i++ {
		e := TestEntitySearch{Name: "Name " + strconv.Itoa(i)}
		entities[i-1] = &e
	}
	err := orm.Flush(entities...)
	assert.Nil(t, err)

	pager := orm.NewPager(1, 100)
	where := orm.NewWhere("`Id` > ? AND `Id` < ?", 1, 8)
	rows := orm.Search(where, pager, TestEntitySearchName)
	assert.Len(t, rows, 6)
	assert.Equal(t, uint(2), rows[0].(TestEntitySearch).Id)
	assert.Equal(t, uint(7), rows[5].(TestEntitySearch).Id)

	pager = orm.NewPager(1, 4)
	rows, totalRows := orm.SearchWithCount(where, pager, TestEntitySearchName)
	assert.Len(t, rows, 4)
	assert.Equal(t, 6, totalRows)
	assert.Equal(t, uint(2), rows[0].(TestEntitySearch).Id)
	assert.Equal(t, uint(5), rows[3].(TestEntitySearch).Id)

	pager = orm.NewPager(2, 4)
	rows, totalRows = orm.SearchWithCount(where, pager, TestEntitySearchName)
	assert.Len(t, rows, 2)
	assert.Equal(t, 6, totalRows)
	assert.Equal(t, uint(6), rows[0].(TestEntitySearch).Id)
	assert.Equal(t, uint(7), rows[1].(TestEntitySearch).Id)

	pager = orm.NewPager(1, 6)
	rows, totalRows = orm.SearchWithCount(where, pager, TestEntitySearchName)
	assert.Len(t, rows, 6)
	assert.Equal(t, 6, totalRows)
	assert.Equal(t, uint(2), rows[0].(TestEntitySearch).Id)
	assert.Equal(t, uint(7), rows[5].(TestEntitySearch).Id)

	ids := orm.SearchIds(where, pager, TestEntitySearchName)
	assert.Len(t, ids, 6)
	assert.Equal(t, uint64(2), ids[0])
	assert.Equal(t, uint64(7), ids[5])
}
