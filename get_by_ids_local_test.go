package orm

import (
	"testing"

	"github.com/apex/log"

	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

type testEntityByIDsLocal struct {
	ORM  `orm:"localCache"`
	ID   uint
	Name string
}

func TestGetByIDsLocal(t *testing.T) {
	var entity testEntityByIDsLocal
	engine := PrepareTables(t, &Registry{}, entity)

	e := &testEntityByIDsLocal{Name: "Hi"}
	engine.Track(e)
	err := engine.Flush()
	assert.Nil(t, err)
	e = &testEntityByIDsLocal{Name: "Hello"}
	engine.Track(e)
	err = engine.Flush()
	assert.Nil(t, err)

	DBLogger := memory.New()
	engine.AddLogger(DBLogger, log.InfoLevel, LoggerSourceDB)
	CacheLogger := memory.New()
	engine.AddLogger(CacheLogger, log.InfoLevel, LoggerSourceLocalCache)

	var found []*testEntityByIDsLocal
	missing, err := engine.LoadByIDs([]uint64{2, 3, 1}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 2)
	assert.Len(t, missing, 1)
	assert.Equal(t, []uint64{3}, missing)
	assert.Equal(t, uint(2), found[0].ID)
	assert.Equal(t, "Hello", found[0].Name)
	assert.Equal(t, uint(1), found[1].ID)
	assert.Equal(t, "Hi", found[1].Name)
	assert.Len(t, DBLogger.Entries, 1)

	missing, err = engine.LoadByIDs([]uint64{2, 3, 1}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 2)
	assert.Len(t, missing, 1)
	assert.Equal(t, []uint64{3}, missing)
	assert.Equal(t, uint(2), found[0].ID)
	assert.Equal(t, uint(1), found[1].ID)
	assert.Len(t, DBLogger.Entries, 1)

	missing, err = engine.LoadByIDs([]uint64{5, 6, 7}, &found)
	assert.Nil(t, err)
	assert.Len(t, found, 0)
	assert.Len(t, missing, 3)
	assert.Len(t, DBLogger.Entries, 2)
}

func BenchmarkGetByIDsLocal(b *testing.B) {
	var entity testEntityByIDsLocal
	engine := PrepareTables(&testing.T{}, &Registry{}, entity)

	e := &testEntityByIDsLocal{Name: "Hi 1"}
	engine.Track(e)
	err := engine.Flush()
	assert.Nil(b, err)
	e = &testEntityByIDsLocal{Name: "Hi 3"}
	engine.Track(e)
	err = engine.Flush()
	assert.Nil(b, err)

	var found []*testEntityByIDsLocal
	for n := 0; n < b.N; n++ {
		_, _ = engine.LoadByIDs([]uint64{1, 2, 3}, &found)
	}
}
