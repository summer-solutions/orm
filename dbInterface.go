package orm

import (
	"database/sql"
)

type DbInterface interface {
	GetDatabaseName(db *sql.DB) (string, error)
	InitDb(db *sql.DB) error
	IsTableEmpty(db *sql.DB, tableName string) (bool, error)
	GetUpdateQuery(tableName string, fields []string) string
	GetInsertQuery(tableName string, fields []string, values string) string
	GetDeleteQuery(tableName string, ids []interface{}) string
	GetDropTableQuery(database string, table string) string
	GetTruncateTableQuery(database string, table string) string
	ConvertToDuplicateKeyError(err error) error
	Limit(pager *Pager) string
	GetAllTables(db *sql.DB) ([]string, error)
	GetSchemaChanges(tableSchema TableSchema) (has bool, alters []Alter, err error)
}
