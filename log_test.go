package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testEntityLog struct {
	ORM  `orm:"log=log"`
	ID   uint
	Name string
	Age  int
}

func TestLog(t *testing.T) {
	entity := &testEntityLog{}
	engine := PrepareTables(t, &Registry{}, entity, entity)
	defer engine.Defer()
	logDB := engine.GetMysql("log")
	_, err := logDB.Exec("TRUNCATE TABLE `_log_default_testEntityLog`")
	assert.Nil(t, err)
	receiver := NewLogReceiver(engine)
	receiver.DisableLoop()

	engine.Track(entity)
	entity.Name = "Hello"
	err = engine.Flush()
	assert.Nil(t, err)

	receiver.Logger = func(value *LogQueueValue) error {
		assert.NotNil(t, value)
		assert.Equal(t, "_log_default_testEntityLog", value.TableName)
		assert.Equal(t, "log", value.PoolName)
		return nil
	}

	err = receiver.Digest()
	assert.NoError(t, err)

	type logRow struct {
		ID       uint64
		EntityID uint64
		AddedAt  string
		Meta     interface{}
		Before   interface{}
		Changes  interface{}
	}
	getLogs := func() []*logRow {
		rows, def, err := logDB.Query("SELECT * FROM `_log_default_testEntityLog` ORDER BY `ID`")
		if def != nil {
			defer def()
		}
		assert.Nil(t, err)
		logs := make([]*logRow, 0)
		for rows.Next() {
			l := &logRow{}
			err := rows.Scan(&l.ID, &l.EntityID, &l.AddedAt, &l.Meta, &l.Before, &l.Changes)
			assert.Nil(t, err)
			logs = append(logs, l)
		}
		return logs
	}

	logs := getLogs()
	assert.Len(t, logs, 1)
	assert.Equal(t, uint64(1), logs[0].ID)
	assert.Nil(t, logs[0].Meta)
	assert.Nil(t, logs[0].Before)
	assert.Equal(t, "{\"Age\": \"0\", \"Name\": \"Hello\"}", string(logs[0].Changes.([]uint8)))

	engine.Track(entity)
	entity.Age = 12
	engine.SetEntityLogMeta("user_name", "john", entity)
	engine.SetLogMetaData("user_id", "7")
	err = engine.Flush()
	assert.Nil(t, err)

	err = receiver.Digest()
	assert.Nil(t, err)
	logs = getLogs()
	assert.Len(t, logs, 2)

	assert.Equal(t, uint64(1), logs[1].EntityID)
	assert.Equal(t, "{\"user_id\": \"7\", \"user_name\": \"john\"}", string(logs[1].Meta.([]uint8)))
	assert.Equal(t, "{\"Age\": \"0\", \"Name\": \"Hello\"}", string(logs[1].Before.([]uint8)))
	assert.Equal(t, "{\"Age\": \"12\"}", string(logs[1].Changes.([]uint8)))

	engine.SetLogMetaData("source", "test")
	engine.Track(entity)
	err = engine.Flush()
	assert.Nil(t, err)

	engine.MarkToDelete(entity)
	err = engine.Flush()
	assert.Nil(t, err)
	err = receiver.Digest()
	assert.Nil(t, err)
	logs = getLogs()
	assert.Len(t, logs, 3)
	assert.Equal(t, uint64(1), logs[2].EntityID)
	assert.Equal(t, "{\"source\": \"test\", \"user_id\": \"7\"}", string(logs[2].Meta.([]uint8)))
	assert.Equal(t, "{\"Age\": \"12\", \"Name\": \"Hello\"}", string(logs[2].Before.([]uint8)))
	assert.Nil(t, logs[2].Changes)
}
