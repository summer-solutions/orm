package tests

import (
	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
	"strconv"
	"testing"
)

type TestEntityFlusherAuto struct {
	Orm  *orm.ORM
	Id   uint
	Name string
}

type TestEntityFlusherManual struct {
	Orm  *orm.ORM
	Id   uint
	Name string
}

func TestFlusherAuto(t *testing.T) {

	var entity TestEntityFlusherAuto
	engine := PrepareTables(t, &orm.Config{}, entity)

	DBLogger := &TestDatabaseLogger{}
	engine.GetMysql().RegisterLogger(DBLogger.Logger())

	flusher := orm.AutoFlusher{Limit: 5}

	for i := 1; i <= 10; i++ {
		e := TestEntityFlusherAuto{Name: "Name " + strconv.Itoa(i)}
		err := flusher.RegisterEntity(engine, &e)
		assert.Nil(t, err)
	}
	assert.Len(t, DBLogger.Queries, 1)
	assert.Equal(t, "INSERT INTO TestEntityFlusherAuto(`Name`) VALUES (?),(?),(?),(?),(?) [Name 1 Name 2 Name 3 Name 4 Name 5]", DBLogger.Queries[0])
	e := TestEntityFlusherAuto{Name: "Name 11"}
	err := flusher.RegisterEntity(engine, &e)
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Queries, 2)
	assert.Equal(t, "INSERT INTO TestEntityFlusherAuto(`Name`) VALUES (?),(?),(?),(?),(?) [Name 6 Name 7 Name 8 Name 9 Name 10]", DBLogger.Queries[1])

	err = flusher.Flush(engine)
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Queries, 3)
	assert.Equal(t, "INSERT INTO TestEntityFlusherAuto(`Name`) VALUES (?) [Name 11]", DBLogger.Queries[2])
}

func TestFlusherManual(t *testing.T) {

	var entity TestEntityFlusherManual
	engine := PrepareTables(t, &orm.Config{}, entity)

	DBLogger := &TestDatabaseLogger{}
	engine.GetMysql().RegisterLogger(DBLogger.Logger())

	flusher := orm.Flusher{Limit: 100}

	for i := 1; i <= 3; i++ {
		e := TestEntityFlusherManual{Name: "Name " + strconv.Itoa(i)}
		flusher.RegisterEntity(&e)
	}
	assert.Len(t, DBLogger.Queries, 0)

	err := flusher.Flush(engine)
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Queries, 1)
	assert.Equal(t, "INSERT INTO TestEntityFlusherManual(`Name`) VALUES (?),(?),(?) [Name 1 Name 2 Name 3]", DBLogger.Queries[0])
}
