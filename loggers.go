package orm

import (
	"github.com/fatih/color"
)

type DatabaseLogger interface {
	Log(mysqlCode string, query string, microseconds int64, args ...interface{})
}

type StandardDatabaseLogger struct {
}

func (l StandardDatabaseLogger) Log(mysqlCode string, query string, microseconds int64, args ...interface{}) {
	if len(args) == 0 {
		color.Green("[ORM][DB][%s] %s\n", mysqlCode, query)
		return
	}
	color.Green("[ORM][DB][%s][%d µs] %s %v\n", mysqlCode, microseconds, query, args)
}

type CacheLogger interface {
	Log(cacheType string, code string, key string, operation string, microseconds int64, misses int)
}

type StandardCacheLogger struct {
}

func (c StandardCacheLogger) Log(cacheType string, code string, key string, operation string, microseconds int64, misses int) {
	if misses == 1 {
		color.Green("[ORM][%s][%s][%d µs][%s] %s [MISS]\n", cacheType, code, microseconds, operation, key)
	} else if misses > 1 {
		color.Green("[ORM][%s][%s][%d µs][%s] %s [MISSES %d]\n", cacheType, code, operation, microseconds, key, misses)
	} else {
		color.Green("[ORM][%s][%s][%d µs][%s] %s\n", cacheType, code, operation, microseconds, key)
	}
}
