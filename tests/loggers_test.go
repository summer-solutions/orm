package tests

import (
	"bytes"
	"log"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/summer-solutions/orm"
)

type TestEntityLoggers struct {
	orm.ORM `orm:"redisCache"`
	ID      uint
}

func TestLoggers(t *testing.T) {
	entity := &TestEntityLoggers{}
	engine := PrepareTables(t, &orm.Registry{}, entity)

	engine.RegisterEntity(entity)
	err := entity.Flush()
	assert.Nil(t, err)

	os.Stdout, _ = os.Open(os.DevNull)

	engine.RegisterDatabaseLogger(&orm.StandardDatabaseLogger{})
	engine.RegisterRedisLogger(&orm.StandardCacheLogger{})

	dbLogger := orm.StandardDatabaseLogger{}
	var bufDB bytes.Buffer
	dbLogger.SetLogger(log.New(&bufDB, "", log.Lshortfile))
	engine.RegisterDatabaseLogger(&dbLogger)

	cacheLogger := orm.StandardCacheLogger{}
	var bufCache bytes.Buffer
	cacheLogger.SetLogger(log.New(&bufCache, "", log.Lshortfile))
	engine.RegisterRedisLogger(&cacheLogger)

	has, err := engine.LoadByID(1, entity)
	assert.Nil(t, err)
	assert.True(t, has)

	assert.Greater(t, strings.Index(bufDB.String(), "SELECT `ID` FROM `TestEntityLoggers` WHERE `ID` = ? LIMIT 1 [1]\n"), 0)
	assert.Greater(t, strings.Index(bufCache.String(), "[GET] TestEntityLoggers1642010974:1 [MISS]"), 0)

	bufCache.Reset()
	has, err = engine.LoadByID(1, entity)
	assert.Nil(t, err)
	assert.True(t, has)
	assert.Equal(t, -1, strings.Index(bufCache.String(), "[GET] TestEntityLoggers1642010974:1 [MISS]"))
	assert.Greater(t, strings.Index(bufCache.String(), "[GET] TestEntityLoggers1642010974:1"), 0)

	bufCache.Reset()
	var entities []*TestEntityLoggers
	missing, err := engine.LoadByIDs([]uint64{10, 11, 12}, &entities)
	assert.Nil(t, err)
	assert.Len(t, missing, 3)
	assert.Greater(t, strings.Index(bufCache.String(), "[MISSES 3]"), 0)

	dbLogger.SetLogger(log.New(&bufDB, "", log.Lshortfile))
}
