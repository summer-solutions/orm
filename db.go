package orm

import (
	"container/list"
	"database/sql"
	"fmt"
	"time"
)

type DB struct {
	db                           *sql.DB
	code                         string
	databaseName                 string
	loggers                      *list.List
	transaction                  *sql.Tx
	transactionCounter           int
	afterCommitLocalCacheSets    map[string][]interface{}
	afterCommitLocalCacheDeletes map[string][]string
	afterCommitRedisCacheDeletes map[string][]string
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	if db.transaction != nil {
		rows, err := db.transaction.Exec(query, args...)
		db.log(query, time.Now().Sub(start).Microseconds(), args...)
		return rows, err
	}
	rows, err := db.db.Exec(query, args...)
	db.log(query, time.Now().Sub(start).Microseconds(), args...)
	return rows, err
}

func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	start := time.Now()
	if db.transaction != nil {
		row := db.transaction.QueryRow(query, args...)
		db.log(query, time.Now().Sub(start).Microseconds(), args...)
		return row
	}
	row := db.db.QueryRow(query, args...)
	db.log(query, time.Now().Sub(start).Microseconds(), args...)
	return row
}

func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	start := time.Now()
	if db.transaction != nil {
		rows, err := db.transaction.Query(query, args...)
		db.log(query, time.Now().Sub(start).Microseconds(), args...)
		return rows, err
	}
	rows, err := db.db.Query(query, args...)
	db.log(query, time.Now().Sub(start).Microseconds(), args...)
	return rows, err
}

func (db *DB) BeginTransaction() error {
	db.transactionCounter++
	if db.transaction != nil {
		return nil
	}
	start := time.Now()
	transaction, err := db.db.Begin()
	db.log("BEGIN TRANSACTION", time.Now().Sub(start).Microseconds())
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
		db.log("COMMIT", time.Now().Sub(start).Microseconds())
		if err == nil {
			if db.afterCommitLocalCacheSets != nil {
				for cacheCode, pairs := range db.afterCommitLocalCacheSets {
					GetLocalCache(cacheCode).MSet(pairs...)
				}
			}
			db.afterCommitLocalCacheSets = nil
			if db.afterCommitLocalCacheDeletes != nil {
				for cacheCode, keys := range db.afterCommitLocalCacheDeletes {
					GetLocalCache(cacheCode).Remove(keys...)
				}
			}
			db.afterCommitLocalCacheDeletes = nil
			if db.afterCommitRedisCacheDeletes != nil {
				for cacheCode, keys := range db.afterCommitRedisCacheDeletes {
					err := GetRedis(cacheCode).Del(keys...)
					if err != nil {
						return err
					}
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
		db.afterCommitLocalCacheSets = nil
		db.afterCommitLocalCacheDeletes = nil
		start := time.Now()
		err := db.transaction.Rollback()
		db.log("ROLLBACK", time.Now().Sub(start).Microseconds())
		if err == nil {
			db.transaction = nil
		}
		return err
	} else {
		return fmt.Errorf("rollback in nested transaction not allowed")
	}
}

func (db *DB) RegisterLogger(logger DatabaseLogger) *list.Element {
	if db.loggers == nil {
		db.loggers = list.New()
	}
	return db.loggers.PushFront(logger)
}

func (db *DB) UnregisterLogger(element *list.Element) {
	db.loggers.Remove(element)
}

func (db *DB) log(query string, microseconds int64, args ...interface{}) {
	if db.loggers != nil {
		for e := db.loggers.Front(); e != nil; e = e.Next() {
			e.Value.(DatabaseLogger)(db.code, query, microseconds, args...)
		}
	}
}
