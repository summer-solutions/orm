package orm

import (
	"context"

	"github.com/go-redis/redis/v8"
)

type RedisSearch struct {
	engine *Engine
	ctx    context.Context
	code   string
	client *redis.Client
}

type RedisSearchIndex struct {
	Name      string
	RedisPool string
}

func (r *RedisSearch) CreateIndex(index RedisSearchIndex) string {
	cmd := redis.NewStringCmd(r.ctx, "FT.CREATE", "myIdx", "ON", "HASH",
		"PREFIX", "1", "doc:", "SCHEMA", "title", "TEXT", "WEIGHT", "5.0", "body",
		"TEXT", "url", "TEXT")
	err := r.client.Process(r.ctx, cmd)
	checkError(err)
	res, err := cmd.Result()
	checkError(err)
	return res
}

type RedisSearchIndexAlter struct {
	Query string
	Safe  bool
	Pool  string
}

func getRedisSearchAlters(engine *Engine) (alters []RedisSearchIndexAlter) {
	return nil
}
