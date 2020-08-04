package orm

import (
	"testing"
	"time"

	apexLog "github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

func TestClickhouse(t *testing.T) {
	registry := &Registry{}
	registry.RegisterClickHouse("http://localhost:9002?debug=false")
	engine := PrepareTables(t, registry)

	testLogger := memory.New()
	engine.AddQueryLogger(testLogger, apexLog.InfoLevel, QueryLoggerSourceClickHouse)
	engine.DataDog().EnableORMAPMLog(apexLog.DebugLevel, true, QueryLoggerSourceClickHouse)

	server := engine.GetClickHouse()
	server.Exec("CREATE DATABASE IF NOT EXISTS test")
	assert.NotNil(t, server.client)

	server.Begin()
	assert.PanicsWithError(t, "transaction already started", func() {
		server.Begin()
	})

	server.Exec("CREATE TABLE IF NOT EXISTS test.hits(`WatchID` UInt64, `UserID` UInt64, `EventDate` Date) ENGINE = MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (WatchID) SETTINGS index_granularity = 8192")
	server.Commit()
	assert.PanicsWithError(t, "transaction not started", func() {
		server.Commit()
	})

	server.Begin()
	server.Exec("INSERT INTO test.hits(WatchID, UserID, EventDate) VALUES(?, ?, ?)", 1, 2, time.Now())
	server.Commit()

	server.Begin()
	prepared, def := server.Prepare("INSERT INTO test.hits(WatchID, UserID, EventDate) VALUES(?, ?, ?)")
	prepared.Exec(1, 3, time.Now())
	def()
	server.Commit()

	total := 0
	rows, def := server.Queryx("SELECT COUNT(*) FROM test.hits")
	assert.True(t, rows.Next())
	err := rows.Scan(&total)
	assert.NoError(t, err)
	assert.Equal(t, 2, total)
	def()

	server.Begin()
	server.Exec("TRUNCATE TABLE test.hits")
	server.Rollback()
	server.Rollback()

	server.Begin()
	assert.Panics(t, func() {
		server.Exec("WRONG QUERY")
	})
	server.Rollback()
}
