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
	PrepareTables(entity)

	Logger := TestDatabaseLogger{}
	orm.GetMysql().AddLogger(&Logger)

	flusher := orm.NewFlusher(5, true)

	for i := 1; i <= 10; i++ {
		e := TestEntityFlusherAuto{Name: "Name " + strconv.Itoa(i)}
		err := flusher.RegisterEntity(&e)
		assert.Nil(t, err)
	}
	assert.Len(t, Logger.Queries, 1)
	assert.Equal(t, "INSERT INTO TestEntityFlusherAuto(`Name`) VALUES (?),(?),(?),(?),(?) [Name 1 Name 2 Name 3 Name 4 Name 5]", Logger.Queries[0])
	e := TestEntityFlusherAuto{Name: "Name 11"}
	err := flusher.RegisterEntity(&e)
	assert.Nil(t, err)
	assert.Len(t, Logger.Queries, 2)
	assert.Equal(t, "INSERT INTO TestEntityFlusherAuto(`Name`) VALUES (?),(?),(?),(?),(?) [Name 6 Name 7 Name 8 Name 9 Name 10]", Logger.Queries[1])

	err = flusher.Flush()
	assert.Nil(t, err)
	assert.Len(t, Logger.Queries, 3)
	assert.Equal(t, "INSERT INTO TestEntityFlusherAuto(`Name`) VALUES (?) [Name 11]", Logger.Queries[2])
}

func TestFlusherManual(t *testing.T) {

	var entity TestEntityFlusherManual
	PrepareTables(entity)

	Logger := TestDatabaseLogger{}
	orm.GetMysql().AddLogger(&Logger)

	flusher := orm.NewFlusher(100, false)

	for i := 1; i <= 3; i++ {
		e := TestEntityFlusherManual{Name: "Name " + strconv.Itoa(i)}
		err := flusher.RegisterEntity(&e)
		assert.Nil(t, err)
	}
	assert.Len(t, Logger.Queries, 0)

	err := flusher.Flush()
	assert.Nil(t, err)
	assert.Len(t, Logger.Queries, 1)
	assert.Equal(t, "INSERT INTO TestEntityFlusherManual(`Name`) VALUES (?),(?),(?) [Name 1 Name 2 Name 3]", Logger.Queries[0])
}
