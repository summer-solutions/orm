package tests

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

type TestEntitySearch struct {
	Orm          *orm.ORM
	ID           uint
	Name         string
	ReferenceOne *orm.ReferenceOne `orm:"ref=tests.TestEntitySearchRef"`
}

type TestEntitySearchRef struct {
	Orm  *orm.ORM `orm:"redisCache"`
	ID   uint
	Name string
}

func TestSearch(t *testing.T) {
	engine := PrepareTables(t, &orm.Registry{}, TestEntitySearch{}, TestEntitySearchRef{})
	var entity TestEntitySearch

	var entities = make([]interface{}, 10)
	var refs = make([]interface{}, 10)
	for i := 1; i <= 10; i++ {
		r := TestEntitySearchRef{Name: "Name " + strconv.Itoa(i)}
		refs[i-1] = &r
		e := TestEntitySearch{Name: "Name " + strconv.Itoa(i)}
		err := engine.Init(&e)
		assert.Nil(t, err)
		e.ReferenceOne.Reference = &r
		entities[i-1] = &e
	}
	err := engine.Flush(refs...)
	assert.Nil(t, err)
	err = engine.Flush(entities...)
	assert.Nil(t, err)

	pager := &orm.Pager{CurrentPage: 1, PageSize: 100}
	where := orm.NewWhere("`ID` > ? AND `ID` < ?", 1, 8)
	var rows []*TestEntitySearch
	err = engine.Search(where, pager, &rows, "ReferenceOne")
	assert.Nil(t, err)
	assert.NotNil(t, rows[0].ReferenceOne.Reference)
	assert.Len(t, rows, 6)
	assert.Equal(t, uint(2), rows[0].ID)
	assert.Equal(t, uint(7), rows[5].ID)

	pager = &orm.Pager{CurrentPage: 1, PageSize: 4}

	rows = make([]*TestEntitySearch, 0)
	totalRows, err := engine.SearchWithCount(where, pager, &rows)
	assert.Nil(t, err)
	assert.Len(t, rows, 4)
	assert.Equal(t, 6, totalRows)
	assert.Equal(t, uint(2), rows[0].ID)
	assert.Equal(t, uint(5), rows[3].ID)

	pager = &orm.Pager{CurrentPage: 2, PageSize: 4}
	rows = make([]*TestEntitySearch, 0)
	totalRows, err = engine.SearchWithCount(where, pager, &rows)
	assert.Nil(t, err)
	assert.Len(t, rows, 2)
	assert.Equal(t, 6, totalRows)
	assert.Equal(t, uint(6), rows[0].ID)
	assert.Equal(t, uint(7), rows[1].ID)

	pager = &orm.Pager{CurrentPage: 1, PageSize: 6}
	rows = make([]*TestEntitySearch, 0)
	totalRows, err = engine.SearchWithCount(where, pager, &rows)
	assert.Nil(t, err)
	assert.Len(t, rows, 6)
	assert.Equal(t, 6, totalRows)
	assert.Equal(t, uint(2), rows[0].ID)
	assert.Equal(t, uint(7), rows[5].ID)

	ids, err := engine.SearchIDs(where, pager, entity)
	assert.Nil(t, err)
	assert.Len(t, ids, 6)
	assert.Equal(t, uint64(2), ids[0])
	assert.Equal(t, uint64(7), ids[5])
}
