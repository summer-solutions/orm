package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testEntityLog struct {
	ORM  `orm:"log=log"`
	ID   uint
	Name string
}

func TestLog(t *testing.T) {
	entity := &testEntityLog{}
	engine := PrepareTables(t, &Registry{}, entity, entity)
	queueRedis, _ := engine.GetRedis("default_log")
	err := queueRedis.FlushDB()
	assert.Nil(t, err)
	logDB, _ := engine.GetMysql("log")

	engine.RegisterNewEntity(entity)
	err = entity.Flush()
	assert.Nil(t, err)
	rows, def, err := logDB.Query("SELECT * FROM `_log_default_testEntityLog`")
	if def != nil {
		defer def()
	}
	assert.Nil(t, err)
	assert.False(t, rows.Next())
	err = rows.Close()
	assert.Nil(t, err)
}
