package orm

import "database/sql"

type testQueryFunc func(db sqlDB, counter int, query string, args ...interface{}) (SQLRows, error)

type testSQLDB struct {
	db        sqlDB
	counter   int
	QueryMock testQueryFunc
}

func (db *testSQLDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.db.Exec(query, args...)
}

func (db *testSQLDB) QueryRow(query string, args ...interface{}) *sql.Row {
	return db.db.QueryRow(query, args...)
}

func (db *testSQLDB) Query(query string, args ...interface{}) (SQLRows, error) {
	db.counter++
	if db.QueryMock != nil {
		return db.QueryMock(db.db, db.counter, query, args...)
	}
	return db.db.Query(query, args...)
}

func mockDBQuery(engine *Engine, poolCode string, f testQueryFunc) *testSQLDB {
	db := engine.dbs[poolCode]
	testDB := &testSQLDB{db: db.db}
	db.db = testDB

	testDB.QueryMock = f
	return testDB
}
