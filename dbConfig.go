package orm

import "database/sql"

type DBConfig struct {
	dataSourceName string
	code           string
	databaseName   string
	db             *sql.DB
}
