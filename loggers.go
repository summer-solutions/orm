package orm

import (
	"github.com/fatih/color"
)

type DatabaseLogger func(mysqlCode string, query string, microseconds int64, args ...interface{})

func NewStandardDatabaseLogger() DatabaseLogger {
	return func(mysqlCode string, query string, microseconds int64, args ...interface{}) {
		if len(args) == 0 {
			color.Green("[ORM][DB][%s] %s\n", mysqlCode, query)
			return
		}
		color.Green("[ORM][DB][%s][%d µs] %s %v\n", mysqlCode, microseconds, query, args)
	}
}

type CacheLogger func(cacheType string, code string, key string, operation string, microseconds int64, misses int)

func NewStandardCacheLogger() CacheLogger {
	return func(cacheType string, code string, key string, operation string, microseconds int64, misses int) {
		if misses == 1 {
			color.Green("[ORM][%s][%s][%d µs][%s] %s [MISS]\n", cacheType, code, microseconds, operation, key)
		} else if misses > 1 {
			color.Green("[ORM][%s][%s][%d µs][%s] %s [MISSES %d]\n", cacheType, code, microseconds, operation, key, misses)
		} else {
			color.Green("[ORM][%s][%s][%d µs][%s] %s\n", cacheType, code, microseconds, operation, key)
		}
	}
}
