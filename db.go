package orm

import (
	"database/sql"
	"time"
)

type sqlDB interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Query(query string, args ...interface{}) (*sql.Rows, error)
	Begin() (*sql.Tx, error)
}

type DB struct {
	engine                       *Engine
	db                           sqlDB
	code                         string
	databaseName                 string
	loggers                      []DatabaseLogger
	transaction                  *sql.Tx
	transactionCounter           int
	afterCommitLocalCacheSets    map[string][]interface{}
	afterCommitLocalCacheDeletes map[string][]string
	afterCommitRedisCacheDeletes map[string][]string
}

func (db *DB) getAfterCommitLocalCacheSets() map[string][]interface{} {
	return db.afterCommitLocalCacheSets
}

func (db *DB) getAfterCommitRedisCacheDeletes() map[string][]string {
	return db.afterCommitRedisCacheDeletes
}

func (db *DB) getDB() sqlDB {
	return db.db
}

func (db *DB) getTransaction() *sql.Tx {
	return db.transaction
}

func (db *DB) GetDatabaseName() string {
	return db.databaseName
}

func (db *DB) GetPoolCode() string {
	return db.code
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
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

func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
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

func (db *DB) Query(query string, args ...interface{}) (rows *sql.Rows, deferF func(), err error) {
	start := time.Now()
	if db.transaction != nil {
		rows, err := db.transaction.Query(query, args...)
		if err != nil {
			return nil, nil, err
		}
		db.log(query, time.Since(start).Microseconds(), args...)
		return rows, func() { rows.Close() }, err
	}
	rows, err = db.db.Query(query, args...)
	if err != nil {
		return nil, nil, err
	}
	db.log(query, time.Since(start).Microseconds(), args...)
	return rows, func() { rows.Close() }, err
}

func (db *DB) BeginTransaction() error {
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

func (db *DB) Commit() error {
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

func (db *DB) Rollback() {
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

func (db *DB) RegisterLogger(logger DatabaseLogger) {
	if db.loggers == nil {
		db.loggers = make([]DatabaseLogger, 0)
	}
	db.loggers = append(db.loggers, logger)
}

func (db *DB) log(query string, microseconds int64, args ...interface{}) {
	if db.loggers != nil {
		for _, logger := range db.loggers {
			logger.Log(db.code, query, microseconds, args...)
		}
	}
}
