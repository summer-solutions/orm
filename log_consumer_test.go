package orm

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type logReceiverEntity1 struct {
	ORM      `orm:"log;asyncRedisLogs=default"`
	ID       uint
	Name     string
	LastName string
	Country  string `orm:"skip-log"`
}

type logReceiverEntity2 struct {
	ORM  `orm:"log"`
	ID   uint
	Name string
	Age  uint64
}

func TestLogReceiver(t *testing.T) {
	var entity1 *logReceiverEntity1
	var entity2 *logReceiverEntity2
	registry := &Registry{}
	engine := PrepareTables(t, registry, 5, entity1, entity2)
	engine.GetMysql().Exec("TRUNCATE TABLE `_log_default_logReceiverEntity1`")
	engine.GetMysql().Exec("TRUNCATE TABLE `_log_default_logReceiverEntity2`")
	engine.GetRedis().FlushDB()

	consumer := NewAsyncConsumer(engine, "default-consumer")
	consumer.DisableLoop()
	consumer.block = time.Millisecond

	e1 := &logReceiverEntity1{Name: "John", LastName: "Smith", Country: "Poland"}
	engine.Flush(e1)
	e2 := &logReceiverEntity2{Name: "Tom", Age: 18}
	engine.Flush(e2)

	valid := false
	validHeartBeat := false
	consumer.SetLogLogger(func(log *LogQueueValue) {
		valid = true
	})
	consumer.SetHeartBeat(time.Minute, func() {
		validHeartBeat = true
	})
	consumer.Digest(context.Background(), 100)
	assert.True(t, valid)
	assert.True(t, validHeartBeat)

	var entityID int
	var meta sql.NullString
	var before sql.NullString
	var changes string
	where1 := NewWhere("SELECT `entity_id`, `meta`, `before`, `changes` FROM `_log_default_logReceiverEntity1` WHERE `ID` = 1")
	engine.GetMysql().QueryRow(where1, &entityID, &meta, &before, &changes)
	assert.Equal(t, 1, entityID)
	assert.False(t, meta.Valid)
	assert.False(t, before.Valid)
	assert.Equal(t, "{\"Name\": \"John\", \"Country\": \"Poland\", \"LastName\": \"Smith\"}", changes)

	where2 := NewWhere("SELECT `entity_id`, `meta`, `before`, `changes` FROM `_log_default_logReceiverEntity2` WHERE `ID` = 1")
	engine.GetMysql().QueryRow(where2, &entityID, &meta, &before, &changes)
	assert.Equal(t, 1, entityID)
	assert.False(t, meta.Valid)
	assert.False(t, before.Valid)
	assert.Equal(t, "{\"Age\": 18, \"Name\": \"Tom\"}", changes)

	engine.SetLogMetaData("user_id", 12)
	flusher := engine.NewFlusher()
	e1 = &logReceiverEntity1{Name: "John2"}
	flusher.Track(e1)
	e2 = &logReceiverEntity2{Name: "Tom2", Age: 18}
	e2.SetEntityLogMeta("admin_id", "10")
	flusher.Track(e2)
	flusher.Flush()

	consumer.Digest(context.Background(), 100)

	where1 = NewWhere("SELECT `entity_id`, `meta`, `before`, `changes` FROM `_log_default_logReceiverEntity1` WHERE `ID` = 2")
	engine.GetMysql().QueryRow(where1, &entityID, &meta, &before, &changes)
	assert.Equal(t, 2, entityID)
	assert.Equal(t, "{\"Name\": \"John2\", \"Country\": null, \"LastName\": null}", changes)
	assert.False(t, before.Valid)
	assert.Equal(t, "{\"user_id\": 12}", meta.String)

	where2 = NewWhere("SELECT `entity_id`, `meta`, `before`, `changes` FROM `_log_default_logReceiverEntity2` WHERE `ID` = 2")
	engine.GetMysql().QueryRow(where2, &entityID, &meta, &before, &changes)
	assert.Equal(t, 2, entityID)
	assert.Equal(t, "{\"Age\": 18, \"Name\": \"Tom2\"}", changes)
	assert.False(t, before.Valid)
	assert.Equal(t, "{\"user_id\": 12, \"admin_id\": \"10\"}", meta.String)

	e1.Country = "Germany"
	engine.Flush(e1)
	consumer.Digest(context.Background(), 100)
	where1 = NewWhere("SELECT `entity_id`, `meta`, `before`, `changes` FROM `_log_default_logReceiverEntity1` WHERE `ID` = 3")
	found := engine.GetMysql().QueryRow(where1, &entityID, &meta, &before, &changes)
	assert.False(t, found)

	e1.LastName = "Summer"
	engine.Flush(e1)
	consumer.Digest(context.Background(), 100)
	where1 = NewWhere("SELECT `entity_id`, `meta`, `before`, `changes` FROM `_log_default_logReceiverEntity1` WHERE `ID` = 3")
	engine.GetMysql().QueryRow(where1, &entityID, &meta, &before, &changes)
	assert.Equal(t, 2, entityID)
	assert.Equal(t, "{\"LastName\": \"Summer\"}", changes)
	assert.Equal(t, "{\"Name\": \"John2\", \"Country\": \"Germany\", \"LastName\": null}", before.String)
	assert.Equal(t, "{\"user_id\": 12}", meta.String)

	engine.Delete(e1)
	consumer.Digest(context.Background(), 100)
	where1 = NewWhere("SELECT `entity_id`, `meta`, `before`, `changes` FROM `_log_default_logReceiverEntity1` WHERE `ID` = 4")
	var changesNullable sql.NullString
	engine.GetMysql().QueryRow(where1, &entityID, &meta, &before, &changesNullable)
	assert.Equal(t, 2, entityID)
	assert.False(t, changesNullable.Valid)
	assert.Equal(t, "{\"Name\": \"John2\", \"Country\": \"Germany\", \"LastName\": \"Summer\"}", before.String)
	assert.Equal(t, "{\"user_id\": 12}", meta.String)
}
