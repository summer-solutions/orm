package orm

import (
	"testing"

	"github.com/apex/log/handlers/memory"

	log2 "github.com/apex/log"
	"github.com/apex/log/handlers/level"

	"github.com/stretchr/testify/assert"
)

func TestEngine(t *testing.T) {
	engine := PrepareTables(t, &Registry{}, 5)
	source := engine.GetRegistry().GetSourceRegistry()
	assert.NotNil(t, source)
	engine.EnableLogger(log2.WarnLevel)
	assert.Len(t, engine.log.logger.handler.Handlers, 1)
	handler, is := engine.log.logger.handler.Handlers[0].(*level.Handler)
	assert.True(t, is)
	assert.Equal(t, log2.WarnLevel, handler.Level)
	engine.AddQueryLogger(memory.New(), log2.InfoLevel)
	assert.Len(t, engine.queryLoggers, 6)

	assert.PanicsWithError(t, "unregistered mysql pool 'test'", func() {
		engine.GetMysql("test")
	})
	assert.PanicsWithError(t, "unregistered local cache pool 'test'", func() {
		engine.GetLocalCache("test")
	})
	assert.PanicsWithError(t, "unregistered redis cache pool 'test'", func() {
		engine.GetRedis("test")
	})
	assert.PanicsWithError(t, "unregistered elastic pool 'test'", func() {
		engine.GetElastic("test")
	})
	assert.PanicsWithError(t, "unregistered clickhouse pool 'test'", func() {
		engine.GetClickHouse("test")
	})
	assert.PanicsWithError(t, "unregistered rabbitMQ queue 'test'", func() {
		engine.GetRabbitMQQueue("test")
	})
	assert.PanicsWithError(t, "unregistered rabbitmq router 'test'", func() {
		engine.GetRabbitMQRouter("test")
	})
	assert.PanicsWithError(t, "unregistered locker pool 'test'", func() {
		engine.GetLocker("test")
	})

	engine.EnableDebug()
	assert.Len(t, engine.log.logger.handler.Handlers, 2)
	handler, is = engine.log.logger.handler.Handlers[1].(*level.Handler)
	assert.True(t, is)
	assert.Equal(t, log2.DebugLevel, handler.Level)

	engine.EnableQueryDebug()
	assert.Len(t, engine.queryLoggers, 6)
	assert.Len(t, engine.queryLoggers[QueryLoggerSourceDB].handler.Handlers, 2)
}

func BenchmarkEngine(b *testing.B) {
	registry := &Registry{}
	validatedRegistry, _ := registry.Validate()
	b.ResetTimer()
	b.ReportAllocs()
	for n := 0; n < b.N; n++ {
		validatedRegistry.CreateEngine()
	}
}
