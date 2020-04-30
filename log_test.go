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
	queueRedis := engine.GetRedis("default_log")
	err := queueRedis.FlushDB()
	assert.Nil(t, err)
	logDB := engine.GetMysql("log")
	_, err = logDB.Exec("TRUNCATE TABLE `_log_default_testEntityLog`")
	assert.Nil(t, err)
	receiver := NewLogReceiver(engine, &RedisQueueSenderReceiver{PoolName: "default_log"})

	codes := engine.GetRegistry().GetLogQueueCodes()
	assert.Equal(t, []string{"log"}, codes)

	engine.Track(entity)
	entity.Name = "Hello"
	err = engine.Flush()
	assert.Nil(t, err)

	size, err := receiver.Size()
	assert.Nil(t, err)
	assert.Equal(t, int64(1), size)
	has, err := receiver.Digest()
	assert.Nil(t, err)
	assert.True(t, has)
	has, err = receiver.Digest()
	assert.Nil(t, err)
	assert.False(t, has)

	type logRow struct {
		ID       uint64
		EntityID uint64
		AddedAt  string
		Meta     interface{}
		Data     interface{}
	}
	getLogs := func() []*logRow {
		rows, def, err := logDB.Query("SELECT * FROM `_log_default_testEntityLog`")
		if def != nil {
			defer def()
		}
		assert.Nil(t, err)
		logs := make([]*logRow, 0)
		for rows.Next() {
			l := &logRow{}
			err := rows.Scan(&l.ID, &l.EntityID, &l.AddedAt, &l.Meta, &l.Data)
			assert.Nil(t, err)
			logs = append(logs, l)
		}
		return logs
	}

	logs := getLogs()
	assert.Len(t, logs, 1)
	assert.Equal(t, uint64(1), logs[0].EntityID)
	assert.Nil(t, logs[0].Meta)
	assert.Equal(t, "{\"Age\": \"0\", \"Name\": \"Hello\"}", string(logs[0].Data.([]uint8)))

	engine.Track(entity)
	entity.Age = 12
	engine.SetLogMetaData("user_id", "7")
	err = engine.Flush()
	assert.Nil(t, err)
	has, err = receiver.Digest()
	assert.Nil(t, err)
	assert.True(t, has)
	logs = getLogs()
	assert.Len(t, logs, 2)
	assert.Equal(t, uint64(1), logs[1].EntityID)
	assert.Equal(t, "{\"user_id\": \"7\"}", string(logs[1].Meta.([]uint8)))
	assert.Equal(t, "{\"Age\": \"12\", \"Name\": \"Hello\"}", string(logs[1].Data.([]uint8)))

	engine.SetLogMetaData("source", "test")
	engine.Track(entity)
	err = engine.Flush()
	assert.Nil(t, err)
	has, err = receiver.Digest()
	assert.Nil(t, err)
	assert.False(t, has)

	engine.MarkToDelete(entity)
	err = engine.Flush()
	assert.Nil(t, err)
	has, err = receiver.Digest()
	assert.Nil(t, err)
	assert.True(t, has)
	logs = getLogs()
	assert.Len(t, logs, 3)
	assert.Equal(t, uint64(1), logs[2].EntityID)
	assert.Equal(t, "{\"source\": \"test\", \"user_id\": \"7\"}", string(logs[2].Meta.([]uint8)))
	assert.Nil(t, logs[2].Data)
}
