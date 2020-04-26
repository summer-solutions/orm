package orm

import (
	"database/sql"
	"github.com/apex/log"
	"github.com/apex/log/handlers/multi"
	"github.com/apex/log/handlers/text"
	"os"
	"time"
)

type sqlDB interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	QueryRow(query string, args ...interface{}) SQLRow
	Query(query string, args ...interface{}) (SQLRows, error)
}

type sqlDBStandard struct {
	db *sql.DB
}

func (db *sqlDBStandard) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.db.Exec(query, args...)
}

func (db *sqlDBStandard) QueryRow(query string, args ...interface{}) SQLRow {
	return db.db.QueryRow(query, args...)
}

func (db *sqlDBStandard) Query(query string, args ...interface{}) (SQLRows, error) {
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
	db           sqlDB
	code         string
	databaseName string
	loggers      []DatabaseLogger
	log          *log.Entry
	logHandler   *multi.Handler
}

func (db *DB) AddLogger(handler log.Handler) {
	db.logHandler.Handlers = append(db.logHandler.Handlers, handler)
}

func (db *DB) SetLogLevel(level log.Level) {
	logger := log.Logger{Handler: db.logHandler, Level: level}
	db.log = logger.WithField("source", "orm")
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

func (db *DB) Exec(query string, args ...interface{}) (rows sql.Result, err error) {
	start := time.Now()
	rows, err = db.db.Exec(query, args...)
	if db.log != nil {
		db.fillLogFields(start, "exec", query, args).Info("[ORM][MYSQL][EXEC]")
	}
	db.logOld(query, time.Since(start).Microseconds(), args...)
	return rows, err
}

func (db *DB) QueryRow(query string, args ...interface{}) SQLRow {
	start := time.Now()
	row := db.db.QueryRow(query, args...)
	if db.log != nil {
		db.fillLogFields(start, "select", query, args).Info("[ORM][MYSQL][SELECT]")
	}
	db.logOld(query, time.Since(start).Microseconds(), args...)
	return row
}

func (db *DB) Query(query string, args ...interface{}) (rows SQLRows, deferF func(), err error) {
	start := time.Now()
	rows, err = db.db.Query(query, args...)
	if db.log != nil {
		db.fillLogFields(start, "select", query, args).Info("[ORM][MYSQL][SELECT]")
	}
	db.logOld(query, time.Since(start).Microseconds(), args...)
	return rows, func() {
		if rows != nil {
			_ = rows.Close()
		}
	}, err
}

func (db *DB) RegisterLogger(logger DatabaseLogger) {
	if db.loggers == nil {
		db.loggers = make([]DatabaseLogger, 0)
	}
	db.loggers = append(db.loggers, logger)
}

func (db *DB) logOld(query string, microseconds int64, args ...interface{}) {
	if db.loggers != nil {
		for _, logger := range db.loggers {
			logger.Log(db.code, query, microseconds, args...)
		}
	}
}

func (db *DB) fillLogFields(start time.Time, typeCode string, query string, args []interface{}) *log.Entry {
	return db.log.
		WithField("pool", db.code).
		WithField("Query", query).
		WithField("args", args).
		WithField("microseconds", time.Since(start).Microseconds()).
		WithField("target", "mysql").
		WithField("type", typeCode).
		WithField("time", start.Unix())
}