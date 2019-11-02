package orm

import (
	"database/sql"
	"fmt"
)

type DB struct {
	db                           *sql.DB
	code                         string
	loggers                      []DatabaseLogger
	transaction                  *sql.Tx
	transactionCounter           int
	afterCommitLocalCacheSets    map[string][]interface{}
	afterCommitLocalCacheDeletes map[string][]string
	afterCommitRedisCacheDeletes map[string][]string
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	db.log(query, args...)
	if db.transaction != nil {
		return db.transaction.Exec(query, args...)
	}
	return db.db.Exec(query, args...)
}

func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	db.log(query, args...)
	if db.transaction != nil {
		return db.transaction.QueryRow(query, args...)
	}
	return db.db.QueryRow(query, args...)
}

func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	db.log(query, args...)
	if db.transaction != nil {
		return db.transaction.Query(query, args...)
	}
	return db.db.Query(query, args...)
}

func (db *DB) BeginTransaction() {
	db.transactionCounter++
	if db.transaction != nil {
		return
	}
	db.log("BEGIN TRANSACTION")
	transaction, err := db.db.ZBegin()
	if err != nil {
		panic(err.Error())
	}
	db.transaction = transaction
}

func (db *DB) Commit() error {
	if db.transaction == nil {
		return nil
	}
	db.transactionCounter--
	if db.transactionCounter == 0 {
		db.log("COMMIT")
		err := db.transaction.Commit()
		if err == nil {
			if db.afterCommitLocalCacheSets != nil {
				for cacheCode, pairs := range db.afterCommitLocalCacheSets {
					GetLocalCacheContainer(cacheCode).MSet(pairs...)
				}
			}
			db.afterCommitLocalCacheSets = nil
			if db.afterCommitLocalCacheDeletes != nil {
				for cacheCode, keys := range db.afterCommitLocalCacheDeletes {
					GetLocalCacheContainer(cacheCode).RemoveMany(keys...)
				}
			}
			db.afterCommitLocalCacheDeletes = nil
			if db.afterCommitRedisCacheDeletes != nil {
				for cacheCode, keys := range db.afterCommitRedisCacheDeletes {
					GetRedisCache(cacheCode).Del(keys...)
				}
			}
			db.afterCommitRedisCacheDeletes = nil
			db.transaction = nil
		}
		return err
	}
	return nil
}

func (db *DB) Rollback() error {
	if db.transaction == nil {
		return nil
	}
	db.transactionCounter--
	if db.transactionCounter == 0 {
		db.log("ROLLBACK")
		db.afterCommitLocalCacheSets = nil
		db.afterCommitLocalCacheDeletes = nil
		err := db.transaction.Rollback()
		if err == nil {
			db.transaction = nil
		}
		return err
	} else {
		return fmt.Errorf("rollback in nested transaction not allowed")
	}
}

func (db *DB) AddLogger(logger DatabaseLogger) {
	if db.loggers == nil {
		db.loggers = make([]DatabaseLogger, 0)
	}
	db.loggers = append(db.loggers, logger)
}

func (db *DB) log(query string, args ...interface{}) {
	if db.loggers != nil {
		for _, logger := range db.loggers {
			logger.Log(db.code, query, args...)
		}
	}
}
