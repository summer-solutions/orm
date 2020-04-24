package tests

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

type TestEntityFlusherAuto struct {
	orm.ORM
	ID   uint
	Name string
}

type TestEntityFlusherManual struct {
	orm.ORM
	ID   uint
	Name string
}

func TestFlusherManual(t *testing.T) {
	var entity TestEntityFlusherManual
	engine := PrepareTables(t, &orm.Registry{}, entity)

	DBLogger := &TestDatabaseLogger{}
	pool := engine.GetMysql()
	pool.RegisterLogger(DBLogger)

	for i := 1; i <= 3; i++ {
		e := TestEntityFlusherManual{Name: "Name " + strconv.Itoa(i)}
		engine.Track(&e)
	}
	assert.Len(t, DBLogger.Queries, 0)

	err := engine.Flush()
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Queries, 1)
	assert.Equal(t, "INSERT INTO TestEntityFlusherManual(`Name`) VALUES (?),(?),(?) [Name 1 Name 2 Name 3]", DBLogger.Queries[0])
}
