package orm

import (
	"testing"

	log2 "github.com/apex/log"
	"github.com/apex/log/handlers/memory"
	"github.com/stretchr/testify/assert"
)

type dbEntity struct {
	ORM
	ID   uint
	Name string
}

func TestDB(t *testing.T) {
	var entity *dbEntity
	engine := PrepareTables(t, &Registry{}, entity)
	logger := memory.New()
	engine.AddQueryLogger(logger, log2.DebugLevel, QueryLoggerSourceDB)

	db := engine.GetMysql()
	db.Exec("INSERT INTO `dbEntity` VALUES(?, ?)", 1, "Tom")

	var id uint64
	var name string
	found := db.QueryRow(NewWhere("SELECT * FROM `dbEntity` WHERE `ID` = ?", 1), &id, &name)
	assert.True(t, found)
	assert.Equal(t, uint64(1), id)
	assert.Equal(t, "Tom", name)

	found = db.QueryRow(NewWhere("SELECT * FROM `dbEntity` WHERE `ID` = ?", 2), &id, &name)
	assert.False(t, found)

	db.Begin()
	db.Exec("INSERT INTO `dbEntity` VALUES(?, ?)", 2, "John")
	db.Rollback()
	db.Rollback()
	found = db.QueryRow(NewWhere("SELECT * FROM `dbEntity` WHERE `ID` = ?", 2), &id, &name)
	assert.False(t, found)

	db.Begin()
	db.Exec("INSERT INTO `dbEntity` VALUES(?, ?)", 2, "John")
	found = db.QueryRow(NewWhere("SELECT * FROM `dbEntity` WHERE `ID` = ?", 2), &id, &name)
	assert.True(t, found)
	rows, def := db.Query("SELECT * FROM `dbEntity` WHERE `ID` > ? ORDER BY `ID`", 0)
	assert.True(t, rows.Next())
	assert.True(t, rows.Next())
	def()
	db.Commit()

	rows, def = db.Query("SELECT * FROM `dbEntity` WHERE `ID` > ? ORDER BY `ID`", 0)
	assert.True(t, rows.Next())
	err := rows.Scan(&id, &name)
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), id)
	assert.Equal(t, "Tom", name)
	assert.True(t, rows.Next())
	err = rows.Scan(&id, &name)
	assert.NoError(t, err)
	assert.Equal(t, uint64(2), id)
	assert.Equal(t, "John", name)
	def()

	assert.PanicsWithError(t, "Error 1064: You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'INVALID QUERY' at line 1", func() {
		db.QueryRow(NewWhere("INVALID QUERY"))
	})

	assert.PanicsWithError(t, "Error 1064: You have an error in your SQL syntax; check the manual that corresponds to your MySQL server version for the right syntax to use near 'INVALID QUERY' at line 1", func() {
		db.Query("INVALID QUERY")
	})

	assert.PanicsWithError(t, "transaction not started", func() {
		db.Commit()
	})
	db.Begin()
	assert.PanicsWithError(t, "transaction already started", func() {
		db.Begin()
	})

	assert.Equal(t, "default", db.GetPoolCode())
	assert.Equal(t, "test", db.GetDatabaseName())

}
