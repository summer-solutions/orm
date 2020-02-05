package orm

import (
	"fmt"
)

type Alter struct {
	Sql  string
	Safe bool
	Pool string
}

func GetAlters() (alters []Alter, err error) {

	tablesInDB := make(map[string]map[string]bool)
	tablesInEntities := make(map[string]map[string]bool)

	for _, pool := range sqlClients {
		poolName := pool.code
		tablesInDB[poolName] = make(map[string]bool)
		results, err := GetMysql(poolName).Query("SHOW TABLES")
		if err != nil {
			return nil, err
		}
		for results.Next() {
			var row string
			err = results.Scan(&row)
			if err != nil {
				return nil, err
			}
			tablesInDB[poolName][row] = true
		}
		tablesInEntities[poolName] = make(map[string]bool)
	}
	alters = make([]Alter, 0)
	for _, t := range entities {
		tableSchema := getTableSchema(t)
		tablesInEntities[tableSchema.MysqlPoolName][tableSchema.TableName] = true
		has, alter, err := tableSchema.GetSchemaChanges()
		if err != nil {
			return nil, err
		}
		if !has {
			continue
		}
		alters = append(alters, alter)
	}

	for poolName, tables := range tablesInDB {
		for tableName := range tables {
			_, has := tablesInEntities[poolName][tableName]
			if !has {
				dropSql := fmt.Sprintf("DROP TABLE `%s`.`%s`;", GetMysql(poolName).databaseName, tableName)
				isEmpty, err := isTableEmptyInPool(poolName, tableName)
				if err != nil {
					return nil, err
				}
				if isEmpty {
					alters = append(alters, Alter{Sql: dropSql, Safe: true, Pool: poolName})
				} else {
					alters = append(alters, Alter{Sql: dropSql, Safe: false, Pool: poolName})
				}
			}
		}
	}
	return
}

func isTableEmptyInPool(poolName string, tableName string) (bool, error) {
	pool := GetMysql(poolName)
	return pool.databaseInterface.IsTableEmpty(pool.db, tableName)
}
