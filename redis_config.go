package orm

import (
	"github.com/go-redis/redis/v7"
)

type RedisCacheConfig struct {
	code   string
	client *redis.Client
}
