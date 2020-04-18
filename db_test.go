package orm

import (
	"database/sql"
	"errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

type testSQLDB struct {
	db sqlDB
	counter int
	QueryMock func(db sqlDB, counter int, query string, args ...interface{}) (*sql.Rows, error)
}

func (db *testSQLDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.db.Exec(query, args...)
}

func (db *testSQLDB) QueryRow(query string, args ...interface{}) *sql.Row {
	return db.db.QueryRow(query, args...)
}

func (db *testSQLDB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	db.counter++
	if db.QueryMock != nil {
		return db.QueryMock(db.db, db.counter, query, args...)
	}
	return db.db.Query(query, args...)
}

func (db *testSQLDB) Begin() (*sql.Tx, error) {
	return db.db.Begin()
}

func TestDBQuery(t *testing.T) {
	registry := new(Registry)
	registry.RegisterMySQLPool("root:root@tcp(localhost:3308)/test")
	config, err := registry.CreateConfig()
	assert.Nil(t, err)
	engine := NewEngine(config)
	db := engine.dbs["default"]
	testDB := &testSQLDB{db: db.db}
	db.db = testDB

	testDB.QueryMock = func(db sqlDB, counter int, query string, args ...interface{}) (*sql.Rows, error) {
		if counter == 2 {
			return nil, errors.New("db error")
		}
		return db.Query(query, args...)
	}
	rows, def, err := db.Query("SELECT 3")
	assert.Nil(t, err)
	assert.NotNil(t, def)
	defer def()
	assert.NotNil(t, rows)
	assert.True(t, rows.Next())
	var data int
	err = rows.Scan(&data)
	assert.Equal(t, 3, data)
	assert.Nil(t, err)
	assert.False(t, rows.Next())

	rows, def, err = db.Query("SELECT 3")
	assert.EqualError(t, err, "db error")
	assert.NotNil(t, def)
	assert.Nil(t, rows)
}
