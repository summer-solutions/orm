package orm

import (
	"strconv"
	"testing"

	log2 "github.com/apex/log"

	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

type testEntityFlusherManual struct {
	ORM
	ID   uint
	Name string
}

func TestFlusherManual(t *testing.T) {
	var entity testEntityFlusherManual
	engine := PrepareTables(t, &Registry{}, entity)

	DBLogger := memory.New()
	engine.AddQueryLogger(DBLogger, log2.InfoLevel, QueryLoggerSourceDB)

	for i := 1; i <= 3; i++ {
		e := testEntityFlusherManual{Name: "Name " + strconv.Itoa(i)}
		engine.Track(&e)
	}
	assert.Len(t, DBLogger.Entries, 0)

	engine.Flush()
	assert.Len(t, DBLogger.Entries, 1)
	assert.Equal(t, "[ORM][MYSQL][EXEC]", DBLogger.Entries[0].Message)
}
