package orm

import (
	"testing"

	log2 "github.com/apex/log"

	"github.com/apex/log/handlers/memory"

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
	engine.AddQueryLogger(testLogger, log2.InfoLevel, QueryLoggerSourceDB)

	db.Begin()

	rows, def := db.Query("SELECT 1")
	defer def()
	assert.True(t, rows.Next())
	def()
	var i int
	db.QueryRow(NewWhere("SELECT 2"), &i)
	assert.Equal(t, 2, i)
	db.Commit()
}
