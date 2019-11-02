package orm

import (
	"github.com/fatih/color"
)

type DatabaseLogger interface {
	Log(mysqlCode string, query string, args ...interface{})
}

type StandardDatabaseLogger struct {
}

func (l StandardDatabaseLogger) Log(mysqlCode string, query string, args ...interface{}) {
	if len(args) == 0 {
		color.Green("[ORM][DB][%s] %s\n", mysqlCode, query)
		return
	}
	color.Green("[ORM][DB][%s] %s %v\n", mysqlCode, query, args)
}

type CacheLogger interface {
	Log(cacheType string, code string, key string, operation string, misses int)
}

type StandardCacheLogger struct {
}

func (c StandardCacheLogger) Log(cacheType string, code string, key string, operation string, misses int) {
	if misses == 1 {
		color.Green("[ORM][%s][%s][%s] %s [MISS]\n", cacheType, code, operation, key)
	} else if misses > 1 {
		color.Green("[ORM][%s][%s][%s] %s [MISSES %d]\n", cacheType, code, operation, key, misses)
	} else {
		color.Green("[ORM][%s][%s][%s] %s\n", cacheType, code, operation, key)
	}
}
