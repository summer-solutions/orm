package orm

import "database/sql"

type testQueryFunc func(db sqlDB, counter int, query string, args ...interface{}) (SQLRows, error)
type testQueryRowFunc func(db sqlDB, counter int, query string, args ...interface{}) SQLRow

type testSQLDB struct {
	db           sqlDB
	counter      int
	QueryMock    testQueryFunc
	QueryRowMock testQueryRowFunc
}

func (db *testSQLDB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.db.Exec(query, args...)
}

func (db *testSQLDB) QueryRow(query string, args ...interface{}) SQLRow {
	return db.db.QueryRow(query, args...)
}

func (db *testSQLDB) Query(query string, args ...interface{}) (SQLRows, error) {
	db.counter++
	if db.QueryMock != nil {
		return db.QueryMock(db.db, db.counter, query, args...)
	}
	return db.db.Query(query, args...)
}

func mockDB(engine *Engine, poolCode string) *testSQLDB {
	db := engine.dbs[poolCode]
	testDB := &testSQLDB{db: db.db}
	db.db = testDB
	return testDB
}
