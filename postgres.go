package orm

import (
	"database/sql"
)

type Postgres struct{}

func (p Postgres) GetDatabaseName(db *sql.DB) (string, error) {
	return "", nil
}

func (p Postgres) InitDb(db *sql.DB) error {
	return nil
}

func (p Postgres) IsTableEmpty(db *sql.DB, tableName string) (bool, error) {
	return false, nil
}

func (p Postgres) GetUpdateQuery(tableName string, fields []string) string {
	return ""
}

func (p Postgres) GetInsertQuery(tableName string, fields []string, values string) string {
	return ""
}

func (p Postgres) GetDeleteQuery(tableName string, ids []interface{}) string {
	return ""
}

func (p Postgres) ConvertToDuplicateKeyError(err error) error {
	return err
}

func (p Postgres) Limit(pager *Pager) string {
	return ""
}

func (p Postgres) GetAllTables(db *sql.DB) ([]string, error) {
	return nil, nil
}

func (p Postgres) GetDropTableQuery(database string, table string) string {
	return ""
}

func (p Postgres) GetTruncateTableQuery(database string, table string) string {
	return ""
}

func (p Postgres) GetSchemaChanges(tableSchema TableSchema) (has bool, alters []Alter, err error) {
	return false, nil, nil
}
