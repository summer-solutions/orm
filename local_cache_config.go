package orm

import (
	"github.com/golang/groupcache/lru"
)

type LocalCacheConfig struct {
	code string
	lru  *lru.Cache
	ttl  int64
}
