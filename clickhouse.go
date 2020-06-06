package orm

import (
	"database/sql"
	"time"

	"github.com/juju/errors"

	"github.com/jmoiron/sqlx"
)

const counterClickHouseAll = "clickhouse.all"
const counterClickHouseQuery = "clickhouse.query"
const counterClickHouseExec = "clickhouse.exec"
const counterClickHouseTransaction = "clickhouse.transaction"

type ClickHouseConfig struct {
	url  string
	code string
	db   *sqlx.DB
}

type ClickHouse struct {
	engine *Engine
	client *sqlx.DB
	tx     *sql.Tx
	code   string
}

func (c *ClickHouse) Exec(query string, args ...interface{}) sql.Result {
	start := time.Now()
	var rows sql.Result
	var err error
	if c.tx != nil {
		rows, err = c.tx.Exec(query, args...)
	} else {
		rows, err = c.client.Exec(query, args...)
	}
	if c.engine.queryLoggers[QueryLoggerSourceClickHouse] != nil {
		c.fillLogFields("[ORM][CLICKHOUSE][EXEC]", start, "exec", query, args, err)
	}
	c.engine.dataDog.incrementCounter(counterClickHouseAll, 1)
	c.engine.dataDog.incrementCounter(counterClickHouseExec, 1)
	if err != nil {
		panic(convertToError(err))
	}
	return rows
}

func (c *ClickHouse) Queryx(query string, args ...interface{}) (rows *sqlx.Rows, deferF func()) {
	start := time.Now()
	rows, err := c.client.Queryx(query, args...)
	if c.engine.queryLoggers[QueryLoggerSourceClickHouse] != nil {
		c.fillLogFields("[ORM][CLICKHOUSE][SELECT]", start, "select", query, args, err)
	}
	c.engine.dataDog.incrementCounter(counterClickHouseAll, 1)
	c.engine.dataDog.incrementCounter(counterClickHouseQuery, 1)
	if err != nil {
		panic(err)
	}
	return rows, func() {
		if rows != nil {
			_ = rows.Close()
		}
	}
}

func (c *ClickHouse) Begin() {
	if c.tx != nil {
		panic(errors.Errorf("transaction already started"))
	}
	start := time.Now()
	tx, err := c.client.Begin()
	if c.engine.queryLoggers[QueryLoggerSourceClickHouse] != nil {
		c.fillLogFields("[ORM][CLICKHOUSE][BEGIN]", start, "transaction", "START TRANSACTION", nil, err)
		c.engine.dataDog.incrementCounter(counterClickHouseAll, 1)
		c.engine.dataDog.incrementCounter(counterClickHouseTransaction, 1)
	}
	if err != nil {
		panic(err)
	}
	c.tx = tx
}

func (c *ClickHouse) Commit() {
	if c.tx == nil {
		panic(errors.Errorf("transaction not started"))
	}
	start := time.Now()
	err := c.tx.Commit()
	if c.engine.queryLoggers[QueryLoggerSourceClickHouse] != nil {
		c.fillLogFields("[ORM][CLICKHOUSE][COMMIT]", start, "transaction", "COMMIT TRANSACTION", nil, err)
		c.engine.dataDog.incrementCounter(counterClickHouseAll, 1)
	}
	if err != nil {
		panic(err)
	}
	c.tx = nil
}

func (c *ClickHouse) Rollback() {
	if c.tx == nil {
		panic(errors.Errorf("transaction not started"))
	}
	start := time.Now()
	err := c.tx.Rollback()
	if c.engine.queryLoggers[QueryLoggerSourceClickHouse] != nil {
		c.fillLogFields("[ORM][CLICKHOUSE][ROLLBACK]", start, "transaction", "ROLLBACK TRANSACTION", nil, err)
		c.engine.dataDog.incrementCounter(counterClickHouseAll, 1)
	}
	if err != nil {
		panic(err)
	}
	c.tx = nil
}

type PreparedStatement struct {
	c         *ClickHouse
	statement *sql.Stmt
	query     string
}

func (p *PreparedStatement) Exec(args ...interface{}) sql.Result {
	start := time.Now()
	results, err := p.statement.Exec(args)
	if p.c.engine.queryLoggers[QueryLoggerSourceClickHouse] != nil {
		p.c.fillLogFields("[ORM][CLICKHOUSE][EXEC]", start, "exec", p.query, args, err)
		p.c.engine.dataDog.incrementCounter(counterClickHouseAll, 1)
		p.c.engine.dataDog.incrementCounter(counterClickHouseExec, 1)
	}
	if err != nil {
		panic(err)
	}
	return results
}

func (c *ClickHouse) Prepare(query string) (prpeparedStatement *PreparedStatement, deferF func()) {
	var err error
	var statement *sql.Stmt
	start := time.Now()
	if c.tx != nil {
		statement, err = c.tx.Prepare(query)
	} else {
		statement, err = c.client.Prepare(query)
	}
	if c.engine.queryLoggers[QueryLoggerSourceClickHouse] != nil {
		c.fillLogFields("[ORM][CLICKHOUSE][PREPARE]", start, "exec", query, nil, err)
		c.engine.dataDog.incrementCounter(counterClickHouseAll, 1)
		c.engine.dataDog.incrementCounter(counterClickHouseExec, 1)
	}
	if err != nil {
		panic(err)
	}
	return &PreparedStatement{c: c, statement: statement, query: query}, func() {
		err := statement.Close()
		if err != nil {
			panic(err)
		}
	}
}

func (c *ClickHouse) fillLogFields(message string, start time.Time, typeCode string, query string, args []interface{}, err error) {
	now := time.Now()
	stop := time.Since(start).Microseconds()
	e := c.engine.queryLoggers[QueryLoggerSourceClickHouse].log.
		WithField("pool", c.code).
		WithField("Query", query).
		WithField("microseconds", stop).
		WithField("target", "mysql").
		WithField("type", typeCode).
		WithField("started", start.UnixNano()).
		WithField("finished", now.UnixNano())
	if args != nil {
		e = e.WithField("args", args)
	}
	if err != nil {
		injectLogError(err, e).Error(message)
	} else {
		e.Info(message)
	}
}
