package orm

import (
	"log"
	"os"
)

type DatabaseLogger interface {
	Log(mysqlCode string, query string, microseconds int64, args ...interface{})
}

type StandardDatabaseLogger struct {
	logger *log.Logger
}

func (logger *StandardDatabaseLogger) SetLogger(log *log.Logger) {
	logger.logger = log
}

func (logger *StandardDatabaseLogger) Log(mysqlCode string, query string, microseconds int64, args ...interface{}) {
	if logger.logger == nil {
		logger.logger = log.New(os.Stdout, "", log.LstdFlags)
	}
	logger.logger.Printf("[ORM][DB][%s][%d µs] %s %v\n", mysqlCode, microseconds, query, args)
}

type CacheLogger interface {
	Log(cacheType string, code string, key string, operation string, microseconds int64, misses int)
}

type StandardCacheLogger struct {
	logger *log.Logger
}

func (logger *StandardCacheLogger) SetLogger(log *log.Logger) {
	logger.logger = log
}

func (logger *StandardCacheLogger) Log(cacheType string, code string, key string, operation string, microseconds int64, misses int) {
	if logger.logger == nil {
		logger.logger = log.New(os.Stdout, "", log.LstdFlags)
	}
	if misses == 1 {
		logger.logger.Printf("[ORM][%s][%s][%d µs][%s] %s [MISS]\n", cacheType, code, microseconds, operation, key)
	} else if misses > 1 {
		logger.logger.Printf("[ORM][%s][%s][%d µs][%s] %s [MISSES %d]\n", cacheType, code, microseconds, operation, key, misses)
	} else {
		logger.logger.Printf("[ORM][%s][%s][%d µs][%s] %s\n", cacheType, code, microseconds, operation, key)
	}
}
