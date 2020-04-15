package tests

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

type TestEntityMySQL struct {
	Orm  *orm.ORM
	ID   uint
	Name string
}

func TestMySQL(t *testing.T) {
	var entity TestEntityMySQL
	engine := PrepareTables(t, &orm.Registry{}, entity, entity)
	for i := 1; i <= 5; i++ {
		e := &TestEntityMySQL{Name: "Name " + strconv.Itoa(i)}
		err := engine.Flush(e)
		assert.Nil(t, err)
	}

	db, has := engine.GetMysql("missing")
	assert.False(t, has)
	assert.Nil(t, db)

	db, has = engine.GetMysql()
	assert.True(t, has)
	assert.NotNil(t, db)

	err := db.BeginTransaction()
	defer db.Rollback()
	assert.Nil(t, err)
	row := db.QueryRow("SELECT * FROM `TestEntityMySQL` WHERE `ID` = ?", 2)
	var id int
	var name string
	err = row.Scan(&id, &name)
	assert.Nil(t, err)

	err = db.Commit()
	assert.Nil(t, err)
	assert.Equal(t, 2, id)
	assert.Equal(t, "Name 2", name)

	err = db.BeginTransaction()
	defer db.Rollback()
	assert.Nil(t, err)
	results, def, err := db.Query("SELECT * FROM `TestEntityMySQL` WHERE `ID` > ? ORDER BY `ID`", 3)
	assert.Nil(t, err)
	assert.NotNil(t, def)
	defer def()
	i := 0
	for results.Next() {
		i++
		err = results.Scan(&id, &name)
		assert.Nil(t, err)
		if i == 1 {
			assert.Equal(t, 4, id)
			assert.Equal(t, "Name 4", name)
		} else {
			assert.Equal(t, 5, id)
			assert.Equal(t, "Name 5", name)
		}
	}
	assert.Equal(t, 2, i)
	err = results.Err()
	assert.Nil(t, err)
	err = db.Commit()
	assert.Nil(t, err)
}
