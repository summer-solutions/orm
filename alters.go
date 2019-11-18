package orm

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
)

func GetAlters() (safeAlters []string, unsafeAlters []string, err error) {

	tablesInDB := make(map[string]map[string]bool)
	tablesInEntities := make(map[string]map[string]bool)

	for _, poolName := range mysqlPoolCodes {
		tablesInDB[poolName] = make(map[string]bool)
		results, err := GetMysql(poolName).Query("SHOW TABLES")
		if err != nil {
			return nil, nil, err
		}
		for results.Next() {
			var row string
			err = results.Scan(&row)
			if err != nil {
				return nil, nil, err
			}
			tablesInDB[poolName][row] = true
		}
		tablesInEntities[poolName] = make(map[string]bool)
	}
	for _, t := range entities {
		tableSchema := getTableSchema(t)
		tablesInEntities[tableSchema.MysqlPoolName][tableSchema.TableName] = true
		has, safeAlter, unsafeAlter, err := tableSchema.GetSchemaChanges()
		if err != nil {
			return nil, nil, err
		}
		if !has {
			continue
		}
		if safeAlter != "" {
			safeAlters = append(safeAlters, safeAlter)
		} else if unsafeAlter != "" {
			unsafeAlters = append(unsafeAlters, unsafeAlter)
		}
	}

	for poolName, tables := range tablesInDB {
		for tableName := range tables {
			_, has := tablesInEntities[poolName][tableName]
			if !has {
				dropSql := fmt.Sprintf("DROP TABLE `%s`;", tableName)
				isEmpty, err := isTableEmptyInPool(poolName, tableName)
				if err != nil {
					return nil, nil, err
				}
				if isEmpty {
					safeAlters = append(safeAlters, dropSql)
				} else {
					unsafeAlters = append(unsafeAlters, dropSql)
				}
			}
		}
	}
	return
}

func isTableEmptyInPool(poolName string, tableName string) (bool, error) {
	var lastId uint64
	err := GetMysql(poolName).QueryRow(fmt.Sprintf("SELECT `Id` FROM `%s` LIMIT 1", tableName)).Scan(&lastId)
	if err != nil {
		if err.Error() == "sql: no rows in result set" {
			return true, nil
		}
		return false, err
	}
	return false, nil
}
