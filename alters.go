package orm

import (
	"database/sql"
	"fmt"
	"sort"
	"strings"
)

type Alter struct {
	SQL  string
	Safe bool
	Pool string
}

func getAlters(engine *Engine) (alters []Alter, err error) {
	tablesInDB := make(map[string]map[string]bool)
	tablesInEntities := make(map[string]map[string]bool)

	if engine.config.sqlClients != nil {
		for _, pool := range engine.config.sqlClients {
			poolName := pool.code
			tablesInDB[poolName] = make(map[string]bool)
			pool, has := engine.GetMysql(poolName)
			if !has {
				return nil, DBPoolNotRegisteredError{Name: poolName}
			}
			tables, err := getAllTables(pool.db)
			if err != nil {
				return nil, err
			}
			for _, table := range tables {
				tablesInDB[poolName][table] = true
			}
			tablesInEntities[poolName] = make(map[string]bool)
		}
	}
	alters = make([]Alter, 0)
	if engine.config.entities != nil {
		for _, t := range engine.config.entities {
			tableSchema := getTableSchema(engine.config, t)
			tablesInEntities[tableSchema.MysqlPoolName][tableSchema.TableName] = true
			has, newAlters, err := tableSchema.GetSchemaChanges(engine)
			if err != nil {
				return nil, err
			}
			if !has {
				continue
			}
			alters = append(alters, newAlters...)
		}
	}

	for poolName, tables := range tablesInDB {
		for tableName := range tables {
			_, has := tablesInEntities[poolName][tableName]
			if !has {
				dropForeignKeyAlter, err := getDropForeignKeysAlter(engine, tableName, poolName)
				if err != nil {
					return nil, err
				}
				if dropForeignKeyAlter != "" {
					alters = append(alters, Alter{SQL: dropForeignKeyAlter, Safe: true, Pool: poolName})
				}
				pool, has := engine.GetMysql(poolName)
				if !has {
					return nil, DBPoolNotRegisteredError{Name: poolName}
				}
				dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS `%s`.`%s`;", pool.databaseName, tableName)
				isEmpty, err := isTableEmptyInPool(engine, poolName, tableName)
				if err != nil {
					return nil, err
				}
				if isEmpty {
					alters = append(alters, Alter{SQL: dropSQL, Safe: true, Pool: poolName})
				} else {
					alters = append(alters, Alter{SQL: dropSQL, Safe: false, Pool: poolName})
				}
			}
		}
	}
	sort.Slice(alters, func(i, j int) bool {
		leftSQL := alters[i].SQL
		rightSQL := alters[j].SQL

		leftHasDropForeignKey := strings.Index(leftSQL, "DROP FOREIGN KEY") > 0
		rightHasDropForeignKey := strings.Index(rightSQL, "DROP FOREIGN KEY") > 0
		if leftHasDropForeignKey && !rightHasDropForeignKey {
			return true
		}
		leftHasAddForeignKey := strings.Index(leftSQL, "ADD CONSTRAINT") > 0
		rightHasAddForeignKey := strings.Index(rightSQL, "ADD CONSTRAINT") > 0
		if !leftHasAddForeignKey && rightHasAddForeignKey {
			return true
		}
		return false
	})
	return
}

func isTableEmptyInPool(engine *Engine, poolName string, tableName string) (bool, error) {
	pool, has := engine.GetMysql(poolName)
	if !has {
		return false, DBPoolNotRegisteredError{Name: poolName}
	}
	return isTableEmpty(pool.db, tableName)
}

func getAllTables(db *sql.DB) ([]string, error) {
	tables := make([]string, 0)
	results, err := db.Query("SHOW TABLES")
	if err != nil {
		return nil, err
	}
	defer results.Close()
	for results.Next() {
		var row string
		err = results.Scan(&row)
		if err != nil {
			return nil, err
		}
		tables = append(tables, row)
	}
	err = results.Err()
	if err != nil {
		return nil, err
	}
	return tables, nil
}

func isTableEmpty(db *sql.DB, tableName string) (bool, error) {
	var lastID uint64
	/* #nosec */
	err := db.QueryRow(fmt.Sprintf("SELECT `ID` FROM `%s` LIMIT 1", tableName)).Scan(&lastID)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return true, nil
		}
		return false, err
	}
	return false, nil
}
