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
	logDB := engine.GetMysql("log")
	_ = logDB.Exec("TRUNCATE TABLE `_log_default_testEntityLog`")
	receiver := NewLogReceiver(engine)
	receiver.DisableLoop()

	engine.Track(entity)
	entity.Name = "Hello"
	engine.Flush()

	receiver.Logger = func(value *LogQueueValue) {
		assert.NotNil(t, value)
		assert.Equal(t, "_log_default_testEntityLog", value.TableName)
		assert.Equal(t, "log", value.PoolName)
	}

	receiver.Digest()

	type logRow struct {
		ID       uint64
		EntityID uint64
		AddedAt  string
		Meta     interface{}
		Before   interface{}
		Changes  interface{}
	}
	getLogs := func() []*logRow {
		rows, def := logDB.Query("SELECT * FROM `_log_default_testEntityLog` ORDER BY `ID`")
		if def != nil {
			defer def()
		}
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
	engine.Flush()

	receiver.Digest()
	logs = getLogs()
	assert.Len(t, logs, 2)

	assert.Equal(t, uint64(1), logs[1].EntityID)
	assert.Equal(t, "{\"user_id\": \"7\", \"user_name\": \"john\"}", string(logs[1].Meta.([]uint8)))
	assert.Equal(t, "{\"Age\": \"0\", \"Name\": \"Hello\"}", string(logs[1].Before.([]uint8)))
	assert.Equal(t, "{\"Age\": \"12\"}", string(logs[1].Changes.([]uint8)))

	engine.SetLogMetaData("source", "test")
	engine.Track(entity)
	engine.Flush()

	engine.MarkToDelete(entity)
	engine.Flush()
	receiver.Digest()
	logs = getLogs()
	assert.Len(t, logs, 3)
	assert.Equal(t, uint64(1), logs[2].EntityID)
	assert.Equal(t, "{\"source\": \"test\", \"user_id\": \"7\"}", string(logs[2].Meta.([]uint8)))
	assert.Equal(t, "{\"Age\": \"12\", \"Name\": \"Hello\"}", string(logs[2].Before.([]uint8)))
	assert.Nil(t, logs[2].Changes)
}
