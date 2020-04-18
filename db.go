package orm

import (
	"database/sql"
	"time"
)

type sqlDB interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) *sql.Row
	Query(query string, args ...interface{}) (*sql.Rows, error)
}

type DB struct {
	engine       *Engine
	db           sqlDB
	code         string
	databaseName string
	loggers      []DatabaseLogger
}

func (db *DB) GetDatabaseName() string {
	return db.databaseName
}

func (db *DB) GetPoolCode() string {
	return db.code
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	start := time.Now()
	rows, err := db.db.Exec(query, args...)
	db.log(query, time.Since(start).Microseconds(), args...)
	return rows, err
}

func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	start := time.Now()
	row := db.db.QueryRow(query, args...)
	db.log(query, time.Since(start).Microseconds(), args...)
	return row
}

func (db *DB) Query(query string, args ...interface{}) (rows *sql.Rows, deferF func(), err error) {
	start := time.Now()
	rows, err = db.db.Query(query, args...)
	if err != nil {
		return nil, func() {}, err
	}
	db.log(query, time.Since(start).Microseconds(), args...)
	return rows, func() { rows.Close() }, err
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
