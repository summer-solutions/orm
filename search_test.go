package orm

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testEntitySearch struct {
	ORM
	ID           uint
	Name         string
	ReferenceOne *testEntitySearchRef
}

type testEntitySearchRef struct {
	ORM  `orm:"redisCache"`
	ID   uint
	Name string
}

func TestSearch(t *testing.T) {
	engine := PrepareTables(t, &Registry{}, testEntitySearch{}, testEntitySearchRef{})
	var entity testEntitySearch

	var entities = make([]interface{}, 10)
	var refs = make([]interface{}, 10)
	for i := 1; i <= 10; i++ {
		r := &testEntitySearchRef{Name: "Name " + strconv.Itoa(i)}
		engine.Track(r)
		refs[i-1] = r
		engine.Flush()
		e := &testEntitySearch{Name: "Name " + strconv.Itoa(i)}
		engine.Track(e)
		e.ReferenceOne = r
		entities[i-1] = e
		engine.Flush()
	}
	pager := &Pager{CurrentPage: 1, PageSize: 100}
	where := NewWhere("`ID` > ?", 1)
	where.Append("AND `ID` < ?", 8)
	var rows []*testEntitySearch
	engine.Search(where, pager, &rows, "ReferenceOne")
	assert.True(t, engine.Loaded(rows[0].ReferenceOne))
	assert.Len(t, rows, 6)
	assert.Equal(t, uint(2), rows[0].ID)
	assert.Equal(t, uint(7), rows[5].ID)

	pager = &Pager{CurrentPage: 1, PageSize: 4}

	rows = make([]*testEntitySearch, 0)
	totalRows := engine.SearchWithCount(where, pager, &rows)
	assert.Len(t, rows, 4)
	assert.Equal(t, 6, totalRows)
	assert.Equal(t, uint(2), rows[0].ID)
	assert.Equal(t, uint(5), rows[3].ID)

	pager = &Pager{CurrentPage: 2, PageSize: 4}
	rows = make([]*testEntitySearch, 0)
	totalRows = engine.SearchWithCount(where, pager, &rows)
	assert.Len(t, rows, 2)
	assert.Equal(t, 6, totalRows)
	assert.Equal(t, uint(6), rows[0].ID)
	assert.Equal(t, uint(7), rows[1].ID)

	pager = &Pager{CurrentPage: 1, PageSize: 6}
	rows = make([]*testEntitySearch, 0)
	totalRows = engine.SearchWithCount(where, pager, &rows)
	assert.Len(t, rows, 6)
	assert.Equal(t, 6, totalRows)
	assert.Equal(t, uint(2), rows[0].ID)
	assert.Equal(t, uint(7), rows[5].ID)

	pager = &Pager{CurrentPage: 1, PageSize: 3}
	ids := engine.SearchIDs(where, pager, &entity)
	assert.Len(t, ids, 3)
	assert.Equal(t, uint64(2), ids[0])

	pager.IncrementPage()
	assert.Equal(t, 2, pager.CurrentPage)
	ids = engine.SearchIDs(where, pager, &entity)
	assert.Len(t, ids, 3)
	assert.Equal(t, uint64(5), ids[0])

	pager = &Pager{CurrentPage: 1, PageSize: 100}
	rows = make([]*testEntitySearch, 0)
	where = NewWhere("(`ID` IN ? OR `ID` IN ?)", []uint{5, 6}, []uint{7, 8})
	totalRows = engine.SearchWithCount(where, pager, &rows)
	assert.Len(t, rows, 4)
	assert.Equal(t, 4, totalRows)

	res, total := engine.SearchIDsWithCount(where, pager, entity)
	assert.Equal(t, 4, total)
	assert.Equal(t, []uint64{5, 6, 7, 8}, res)

	has := engine.SearchOne(NewWhere("`ID` = 1"), &entity, "ReferenceOne")
	assert.True(t, has)
	assert.NotNil(t, entity.ReferenceOne)
	assert.Equal(t, "Name 1", entity.ReferenceOne.Name)
}
