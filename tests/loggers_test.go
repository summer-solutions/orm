package tests

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"log"
	"strings"
	"testing"

	"github.com/summer-solutions/orm"
)

type TestEntityLoggers struct {
	Orm                  *orm.ORM `orm:"redisCache"`
	ID                   uint
}

func TestLoggers(t *testing.T) {
	var entity TestEntityLoggers
	engine := PrepareTables(t, &orm.Registry{}, entity)

	dbLogger := orm.StandardDatabaseLogger{}
	var bufDB bytes.Buffer
	dbLogger.SetLogger(log.New(&bufDB, "", log.Lshortfile))
	engine.RegisterDatabaseLogger(&dbLogger)

	cacheLogger := orm.StandardCacheLogger{}
	var bufCache bytes.Buffer
	cacheLogger.SetLogger(log.New(&bufCache, "", log.Lshortfile))
	engine.RegisterRedisLogger(&cacheLogger)

	has, err := engine.TryByID(1, &entity)
	assert.Nil(t, err)
	assert.False(t, has)

	assert.Greater(t, strings.Index(bufDB.String(), "SELECT `ID` FROM `TestEntityLoggers` WHERE `ID` = ? LIMIT 1 [1]\n"), 0)
	assert.Greater(t, strings.Index(bufCache.String(), "[GET] TestEntityLoggers1642010974:1 [MISS]"), 0)
}
