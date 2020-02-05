package orm

import (
	"database/sql"
	"fmt"
	"github.com/go-sql-driver/mysql"
	_ "github.com/go-sql-driver/mysql"
	"math"
	"regexp"
	"strings"
	"time"
)

type Mysql struct{}

func (m Mysql) GetDatabaseName(db *sql.DB) (string, error) {
	var dbName string
	err := db.QueryRow("SELECT DATABASE()").Scan(&dbName)
	if err != nil {
		return "", err
	}
	return dbName, nil
}

func (m Mysql) InitDb(db *sql.DB) error {
	var variable string
	var maxConnections float64
	var maxTime float64
	err := db.QueryRow("SHOW VARIABLES LIKE 'max_connections'").Scan(&variable, &maxConnections)
	if err != nil {
		return err
	}
	err = db.QueryRow("SHOW VARIABLES LIKE 'interactive_timeout'").Scan(&variable, &maxTime)
	if err != nil {
		return err
	}
	maxConnectionsOrm := math.Ceil(maxConnections * 0.9)
	maxIdleConnections := math.Ceil(maxConnections * 0.05)
	maxConnectionsTime := math.Ceil(maxTime * 0.7)
	if maxIdleConnections < 10 {
		maxIdleConnections = maxConnectionsOrm
	}

	db.SetMaxOpenConns(int(maxConnectionsOrm))
	db.SetMaxIdleConns(int(maxIdleConnections))
	db.SetConnMaxLifetime(time.Duration(int(maxConnectionsTime)) * time.Second)
	return nil
}

func (m Mysql) IsTableEmpty(db *sql.DB, tableName string) (bool, error) {
	var lastId uint64
	err := db.QueryRow(fmt.Sprintf("SELECT `Id` FROM `%s` LIMIT 1", tableName)).Scan(&lastId)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return true, nil
		}
		return false, err
	}
	return false, nil
}

func (m Mysql) GetUpdateQuery(tableName string, fields []string) string {
	return fmt.Sprintf("UPDATE %s SET %s WHERE `Id` = ?", tableName, strings.Join(fields, ","))
}

func (m Mysql) GetInsertQuery(tableName string, fields []string, values string) string {
	return fmt.Sprintf("INSERT INTO %s(%s) VALUES %s", tableName, strings.Join(fields, ","), values)
}

func (m Mysql) GetDeleteQuery(tableName string, ids []interface{}) string {
	where := NewWhere("`Id` IN ?", ids)
	return fmt.Sprintf("DELETE FROM `%s` WHERE %s", tableName, where)
}

func (m Mysql) ConvertToDuplicateKeyError(err error) error {
	sqlErr, yes := err.(*mysql.MySQLError)
	if yes && sqlErr.Number == 1062 {
		var abortLabelReg, _ = regexp.Compile(` for key '(.*?)'`)
		labels := abortLabelReg.FindStringSubmatch(sqlErr.Message)
		if len(labels) > 0 {
			return &DuplicatedKeyError{Message: sqlErr.Message, Index: labels[1]}
		}
	}
	return err
}

func (m Mysql) Limit(pager Pager) string {
	return fmt.Sprintf("LIMIT %d,%d", (pager.CurrentPage-1)*pager.PageSize, pager.PageSize)
}
