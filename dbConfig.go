package orm

import (
	"database/sql"
)

type DBConfig struct {
	db           *sql.DB
	code         string
	databaseName string
}
