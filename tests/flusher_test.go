package tests

import (
	"strconv"
	"testing"

	"github.com/apex/log"

	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

type TestEntityFlusherManual struct {
	orm.ORM
	ID   uint
	Name string
}

func TestFlusherManual(t *testing.T) {
	var entity TestEntityFlusherManual
	engine := PrepareTables(t, &orm.Registry{}, entity)

	DBLogger := memory.New()
	pool := engine.GetMysql()
	pool.AddLogger(DBLogger)
	pool.SetLogLevel(log.InfoLevel)

	for i := 1; i <= 3; i++ {
		e := TestEntityFlusherManual{Name: "Name " + strconv.Itoa(i)}
		engine.Track(&e)
	}
	assert.Len(t, DBLogger.Entries, 0)

	err := engine.Flush()
	assert.Nil(t, err)
	assert.Len(t, DBLogger.Entries, 1)
	assert.Equal(t, "[ORM][MYSQL][EXEC]", DBLogger.Entries[0].Message)
}
