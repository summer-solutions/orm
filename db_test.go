package orm

import (
	"github.com/apex/log/handlers/memory"
	"testing"

	"github.com/apex/log"
	"github.com/stretchr/testify/assert"
)

func TestDB(t *testing.T) {
	registry := &Registry{}
	registry.RegisterMySQLPool("root:root@tcp(localhost:3308)/test")
	registry.RegisterRabbitMQServer("amqp://rabbitmq_user:rabbitmq_password@localhost:5672/test")
	validatedRegistry, err := registry.Validate()
	assert.Nil(t, err)
	engine := validatedRegistry.CreateEngine()
	db := engine.GetMysql()
	assert.NotNil(t, db)
	testLogger := memory.New()
	engine.AddLogger(testLogger, log.InfoLevel, LoggerSourceDB)

	err = db.Commit()
	assert.EqualError(t, err, "transaction not started")
	err = db.Begin()
	assert.Nil(t, err)
	err = db.Begin()
	assert.EqualError(t, err, "transaction already started")
	db.Rollback()

	err = db.Begin()
	assert.Nil(t, err)

	rows, def, err := db.Query("SELECT 1")
	assert.Nil(t, err)
	defer def()
	assert.True(t, rows.Next())
	def()
	row := db.QueryRow("SELECT 2")
	var i int
	err = row.Scan(&i)
	assert.Nil(t, err)
	assert.Equal(t, 2, i)
	err = db.Commit()
	assert.Nil(t, err)
}
