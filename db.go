package orm

import (
	"database/sql"
	"fmt"
	"os"
	"time"

	"github.com/juju/errors"

	"github.com/apex/log"
	"github.com/apex/log/handlers/multi"
	"github.com/apex/log/handlers/text"
)

type sqlClient interface {
	Begin() error
	Commit() error
	Rollback() (bool, error)
	Exec(query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) SQLRow
	Query(query string, args ...interface{}) (SQLRows, error)
}

type standardSQLClient struct {
	db *sql.DB
	tx *sql.Tx
}

func (db *standardSQLClient) Begin() error {
	if db.tx != nil {
		return fmt.Errorf("transaction already started")
	}
	tx, err := db.db.Begin()
	db.tx = tx
	return err
}

func (db *standardSQLClient) Commit() error {
	if db.tx == nil {
		return fmt.Errorf("transaction not started")
	}
	err := db.tx.Commit()
	if err != nil {
		return err
	}
	db.tx = nil
	return nil
}

func (db *standardSQLClient) Rollback() (bool, error) {
	if db.tx == nil {
		return false, nil
	}
	err := db.tx.Rollback()
	if err != nil {
		return true, err
	}
	db.tx = nil
	return true, nil
}

func (db *standardSQLClient) Exec(query string, args ...interface{}) (sql.Result, error) {
	if db.tx != nil {
		return db.tx.Exec(query, args...)
	}
	return db.db.Exec(query, args...)
}

func (db *standardSQLClient) QueryRow(query string, args ...interface{}) SQLRow {
	if db.tx != nil {
		return db.tx.QueryRow(query, args...)
	}
	return db.db.QueryRow(query, args...)
}

func (db *standardSQLClient) Query(query string, args ...interface{}) (SQLRows, error) {
	if db.tx != nil {
		return db.tx.Query(query, args...)
	}
	return db.db.Query(query, args...)
}

type SQLRows interface {
	Next() bool
	Err() error
	Close() error
	Scan(dest ...interface{}) error
	Columns() ([]string, error)
}

type SQLRow interface {
	Scan(dest ...interface{}) error
}

type DB struct {
	engine       *Engine
	client       sqlClient
	code         string
	databaseName string
	log          *log.Entry
	logHandler   *multi.Handler
}

func (db *DB) AddLogger(handler log.Handler) {
	db.logHandler.Handlers = append(db.logHandler.Handlers, handler)
}

func (db *DB) SetLogLevel(level log.Level) {
	logger := log.Logger{Handler: db.logHandler, Level: level}
	db.log = logger.WithField("source", "orm")
	db.log.Level = level
}

func (db *DB) EnableDebug() {
	db.AddLogger(text.New(os.Stdout))
	db.SetLogLevel(log.DebugLevel)
}

func (db *DB) GetDatabaseName() string {
	return db.databaseName
}

func (db *DB) GetPoolCode() string {
	return db.code
}

func (db *DB) Begin() error {
	start := time.Now()
	err := db.client.Begin()
	if err != nil {
		return err
	}
	if db.log != nil {
		db.fillLogFields(start, "transaction", "START TRANSACTION", nil).Info("[ORM][MYSQL][BEGIN]")
	}
	return nil
}

func (db *DB) Commit() error {
	start := time.Now()
	err := db.client.Commit()
	if err != nil {
		return err
	}
	if db.log != nil {
		db.fillLogFields(start, "transaction", "COMMIT", nil).Info("[ORM][MYSQL][COMMIT]")
	}
	if db.engine.afterCommitLocalCacheSets != nil {
		for cacheCode, pairs := range db.engine.afterCommitLocalCacheSets {
			cache := db.engine.GetLocalCache(cacheCode)
			cache.MSet(pairs...)
		}
	}
	db.engine.afterCommitLocalCacheSets = nil
	if db.engine.afterCommitRedisCacheDeletes != nil {
		for cacheCode, keys := range db.engine.afterCommitRedisCacheDeletes {
			cache := db.engine.GetRedis(cacheCode)
			err := cache.Del(keys...)
			if err != nil {
				return err
			}
		}
	}
	db.engine.afterCommitRedisCacheDeletes = nil
	return nil
}

func (db *DB) Rollback() {
	start := time.Now()
	has, err := db.client.Rollback()
	if err != nil {
		panic(errors.Annotate(err, "rollback failed"))
	}
	if has && db.log != nil {
		db.fillLogFields(start, "transaction", "ROLLBACK", nil).Info("[ORM][MYSQL][ROLLBACK]")
	}
	db.engine.afterCommitLocalCacheSets = nil
	db.engine.afterCommitRedisCacheDeletes = nil
}

func (db *DB) Exec(query string, args ...interface{}) (rows sql.Result, err error) {
	start := time.Now()
	rows, err = db.client.Exec(query, args...)
	if db.log != nil {
		db.fillLogFields(start, "exec", query, args).Info("[ORM][MYSQL][EXEC]")
	}
	return rows, err
}

func (db *DB) QueryRow(query string, args ...interface{}) SQLRow {
	start := time.Now()
	row := db.client.QueryRow(query, args...)
	if db.log != nil {
		db.fillLogFields(start, "select", query, args).Info("[ORM][MYSQL][SELECT]")
	}
	return row
}

func (db *DB) Query(query string, args ...interface{}) (rows SQLRows, deferF func(), err error) {
	start := time.Now()
	rows, err = db.client.Query(query, args...)
	if db.log != nil {
		db.fillLogFields(start, "select", query, args).Info("[ORM][MYSQL][SELECT]")
	}
	return rows, func() {
		if rows != nil {
			_ = rows.Close()
		}
	}, err
}

func (db *DB) fillLogFields(start time.Time, typeCode string, query string, args []interface{}) *log.Entry {
	e := db.log.
		WithField("pool", db.code).
		WithField("Query", query).
		WithField("microseconds", time.Since(start).Microseconds()).
		WithField("target", "mysql").
		WithField("type", typeCode).
		WithField("time", start.Unix())
	if args != nil {
		e = e.WithField("args", args)
	}
	return e
}
