package orm

import (
	"database/sql"
	"time"
)

type db struct {
	engine                       *Engine
	db                           *sql.DB
	code                         string
	databaseName                 string
	loggers                      []DatabaseLogger
	transaction                  *sql.Tx
	transactionCounter           int
	afterCommitLocalCacheSets    map[string][]interface{}
	afterCommitLocalCacheDeletes map[string][]string
	afterCommitRedisCacheDeletes map[string][]string
}

type DB interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Query(query string, args ...interface{}) (rows *sql.Rows, deferF func(), err error)
	BeginTransaction() error
	Commit() error
	Rollback()
	RegisterLogger(logger DatabaseLogger)
	GetPoolCode() string
	GetDatabaseName() string
	getDB() *sql.DB
	getTransaction() *sql.Tx
	getAfterCommitLocalCacheSets() map[string][]interface{}
	getAfterCommitRedisCacheDeletes() map[string][]string
}

func (db *db) getAfterCommitLocalCacheSets() map[string][]interface{} {
	return db.afterCommitLocalCacheSets
}

func (db *db) getAfterCommitRedisCacheDeletes() map[string][]string {
	return db.afterCommitRedisCacheDeletes
}

func (db *db) getDB() *sql.DB {
	return db.db
}

func (db *db) getTransaction() *sql.Tx {
	return db.transaction
}

func (db *db) GetDatabaseName() string {
	return db.databaseName
}

func (db *db) GetPoolCode() string {
	return db.code
}

func (db *db) Exec(query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	if db.transaction != nil {
		rows, err := db.transaction.Exec(query, args...)
		db.log(query, time.Since(start).Microseconds(), args...)
		return rows, err
	}
	rows, err := db.db.Exec(query, args...)
	db.log(query, time.Since(start).Microseconds(), args...)
	return rows, err
}

func (db *db) QueryRow(query string, args ...interface{}) *sql.Row {
	start := time.Now()
	if db.transaction != nil {
		row := db.transaction.QueryRow(query, args...)
		db.log(query, time.Since(start).Microseconds(), args...)
		return row
	}
	row := db.db.QueryRow(query, args...)
	db.log(query, time.Since(start).Microseconds(), args...)
	return row
}

func (db *db) Query(query string, args ...interface{}) (rows *sql.Rows, deferF func(), err error) {
	start := time.Now()
	if db.transaction != nil {
		rows, err := db.transaction.Query(query, args...)
		db.log(query, time.Since(start).Microseconds(), args...)
		if err != nil {
			return nil, nil, err
		}
		return rows, func() { rows.Close() }, err
	}
	rows, err = db.db.Query(query, args...)
	if err != nil {
		return nil, nil, err
	}
	db.log(query, time.Since(start).Microseconds(), args...)
	return rows, func() { rows.Close() }, err
}

func (db *db) BeginTransaction() error {
	db.transactionCounter++
	if db.transaction != nil {
		return nil
	}
	start := time.Now()
	transaction, err := db.db.Begin()
	db.log("BEGIN TRANSACTION", time.Since(start).Microseconds())
	if err != nil {
		return err
	}
	db.transaction = transaction
	return nil
}

func (db *db) Commit() error {
	if db.transaction == nil {
		return nil
	}
	db.transactionCounter--
	if db.transactionCounter == 0 {
		start := time.Now()
		err := db.transaction.Commit()
		db.log("COMMIT", time.Since(start).Microseconds())
		if err == nil {
			if len(db.afterCommitLocalCacheSets) > 0 {
				for cacheCode, pairs := range db.afterCommitLocalCacheSets {
					cache, _ := db.engine.GetLocalCache(cacheCode)
					cache.MSet(pairs...)
				}
				db.afterCommitLocalCacheSets = make(map[string][]interface{})
			}
			if len(db.afterCommitLocalCacheDeletes) > 0 {
				for cacheCode, keys := range db.afterCommitLocalCacheDeletes {
					cache, _ := db.engine.GetLocalCache(cacheCode)
					cache.Remove(keys...)
				}
				db.afterCommitLocalCacheDeletes = make(map[string][]string)
			}

			if len(db.afterCommitRedisCacheDeletes) > 0 {
				for cacheCode, keys := range db.afterCommitRedisCacheDeletes {
					cache, _ := db.engine.GetRedis(cacheCode)
					err := cache.Del(keys...)
					if err != nil {
						return err
					}
				}
				db.afterCommitRedisCacheDeletes = make(map[string][]string)
			}
			db.transaction = nil
		}
		return err
	}
	return nil
}

func (db *db) Rollback() {
	if db.transaction == nil {
		return
	}
	db.transactionCounter--
	if db.transactionCounter == 0 {
		db.afterCommitLocalCacheSets = nil
		db.afterCommitLocalCacheDeletes = nil
		start := time.Now()
		err := db.transaction.Rollback()
		db.log("ROLLBACK", time.Since(start).Microseconds())
		if err == nil {
			db.transaction = nil
		}
	}
}

func (db *db) RegisterLogger(logger DatabaseLogger) {
	if db.loggers == nil {
		db.loggers = make([]DatabaseLogger, 0)
	}
	db.loggers = append(db.loggers, logger)
}

func (db *db) log(query string, microseconds int64, args ...interface{}) {
	if db.loggers != nil {
		for _, logger := range db.loggers {
			logger.Log(db.code, query, microseconds, args...)
		}
	}
}
