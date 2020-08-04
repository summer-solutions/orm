package orm

import (
	"testing"

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
	server.Exec("CREATE TABLE IF NOT EXISTS test.hits(`WatchID` UInt64, `UserID` UInt64, `EventDate` Date) ENGINE = MergeTree() PARTITION BY toYYYYMM(EventDate) ORDER BY (WatchID) SETTINGS index_granularity = 8192")
	server.Commit()

	server.Begin()
	server.Exec("TRUNCATE TABLE test.hits")
	server.Rollback()
	server.Rollback()
}
