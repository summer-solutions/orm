package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"strconv"
	"testing"
)

type TestEntitySearch struct {
	Orm          *orm.ORM
	Id           uint
	Name         string
	ReferenceOne *orm.ReferenceOne `orm:"ref=tests.TestEntitySearchRef"`
}

type TestEntitySearchRef struct {
	Orm  *orm.ORM `orm:"redisCache"`
	Id   uint
	Name string
}

func TestSearch(t *testing.T) {

	PrepareTables(TestEntitySearch{}, TestEntitySearchRef{})
	var entity TestEntitySearch

	var entities = make([]interface{}, 10)
	var refs = make([]interface{}, 10)
	for i := 1; i <= 10; i++ {
		r := TestEntitySearchRef{Name: "Name " + strconv.Itoa(i)}
		refs[i-1] = &r
		e := TestEntitySearch{Name: "Name " + strconv.Itoa(i)}
		err := orm.Init(&e)
		assert.Nil(t, err)
		e.ReferenceOne.Reference = &r
		entities[i-1] = &e
	}
	err := orm.Flush(refs...)
	assert.Nil(t, err)
	err = orm.Flush(entities...)
	assert.Nil(t, err)

	pager := &orm.Pager{CurrentPage: 1, PageSize: 100}
	where := orm.NewWhere("`Id` > ? AND `Id` < ?", 1, 8)
	var rows []*TestEntitySearch
	err = orm.Search(where, pager, &rows, "ReferenceOne")
	assert.Nil(t, err)
	assert.NotNil(t, rows[0].ReferenceOne.Reference)
	assert.Len(t, rows, 6)
	assert.Equal(t, uint(2), rows[0].Id)
	assert.Equal(t, uint(7), rows[5].Id)

	pager = &orm.Pager{CurrentPage: 1, PageSize: 4}

	rows = make([]*TestEntitySearch, 0)
	totalRows, err := orm.SearchWithCount(where, pager, &rows)
	assert.Nil(t, err)
	assert.Len(t, rows, 4)
	assert.Equal(t, 6, totalRows)
	assert.Equal(t, uint(2), rows[0].Id)
	assert.Equal(t, uint(5), rows[3].Id)

	pager = &orm.Pager{CurrentPage: 2, PageSize: 4}
	rows = make([]*TestEntitySearch, 0)
	totalRows, err = orm.SearchWithCount(where, pager, &rows)
	assert.Nil(t, err)
	assert.Len(t, rows, 2)
	assert.Equal(t, 6, totalRows)
	assert.Equal(t, uint(6), rows[0].Id)
	assert.Equal(t, uint(7), rows[1].Id)

	pager = &orm.Pager{CurrentPage: 1, PageSize: 6}
	rows = make([]*TestEntitySearch, 0)
	totalRows, err = orm.SearchWithCount(where, pager, &rows)
	assert.Nil(t, err)
	assert.Len(t, rows, 6)
	assert.Equal(t, 6, totalRows)
	assert.Equal(t, uint(2), rows[0].Id)
	assert.Equal(t, uint(7), rows[5].Id)

	ids := orm.SearchIds(where, pager, entity)
	assert.Len(t, ids, 6)
	assert.Equal(t, uint64(2), ids[0])
	assert.Equal(t, uint64(7), ids[5])
}
