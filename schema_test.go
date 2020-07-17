package orm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type schemaSubFields struct {
	Name string
	Age  uint16
}

type schemaEntityRef struct {
	ORM  `orm:"log"`
	ID   uint
	Name string
}

type testEnum struct {
	EnumModel
	A string
	B string
	C string
}

var TestEnum = &testEnum{
	A: "a",
	B: "b",
	C: "c",
}

type schemaEntity struct {
	ORM            `orm:"log"`
	ID             uint
	Name           string `orm:"required;index=TestIndex"`
	Year           uint16 `orm:"year"`
	Uint8          uint8
	Uint16         uint16 `orm:"required;index=TestIndex:2"`
	Uint32         uint32
	Uint32Medium   uint32 `orm:"mediumint"`
	YearRequired   uint16 `orm:"year;required"`
	Uint64         uint64
	Int8           int8 `orm:"unique=TestUniqueIndex"`
	Int16          int16
	Int32          int32
	Int32Medium    int32 `orm:"mediumint"`
	Int64          int64
	Int            int
	Bool           bool
	Interface      interface{}
	Float32        float32
	Float64        float64
	Time           time.Time
	TimeFull       time.Time `orm:"time"`
	TimeNull       *time.Time
	Blob           []uint8
	MediumBlob     []uint8 `orm:"mediumblob"`
	LongBlob       []uint8 `orm:"longblob"`
	SubStruct      schemaSubFields
	CachedQuery    *CachedQuery
	Ignored        string `orm:"ignore"`
	NameTranslated map[string]string
	RefOne         *schemaEntityRef
	RefOneCascade  *schemaEntityRef `orm:"cascade"`
	Decimal        float32          `orm:"decimal=10,2"`
	Enum           string           `orm:"enum=orm.TestEnum;required"`
	Set            string           `orm:"set=orm.TestEnum;required"`
	FakeDelete     bool
}

func TestSchema(t *testing.T) {
	entity := &schemaEntity{}
	ref := &schemaEntityRef{}
	registry := &Registry{}
	registry.RegisterEnumStruct("orm.TestEnum", TestEnum)
	engine := PrepareTables(t, registry, entity, ref)

	engineDrop := PrepareTables(t, &Registry{})
	for _, alter := range engineDrop.GetAlters() {
		engineDrop.GetMysql(alter.Pool).Exec(alter.SQL)
	}

	//schema = engine.GetRegistry().GetTableSchemaForEntity(entity)
	alters := engine.GetAlters()
	assert.Len(t, alters, 5)
	assert.True(t, alters[0].Safe)
	assert.True(t, alters[1].Safe)
	assert.True(t, alters[2].Safe)
	assert.True(t, alters[3].Safe)
	assert.True(t, alters[4].Safe)
	assert.Equal(t, "default", alters[0].Pool)
	assert.Equal(t, "CREATE TABLE `test`.`schemaEntityRef` (\n  `ID` int(10) unsigned NOT NULL AUTO_INCREMENT,\n  `Name` varchar(255) DEFAULT NULL,\n  PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8;", alters[0].SQL)
	assert.Equal(t, "CREATE TABLE `test`.`_log_default_schemaEntity` (\n  `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,\n  `entity_id` int(10) unsigned NOT NULL,\n  `added_at` datetime NOT NULL,\n  `meta` json DEFAULT NULL,\n  `before` json DEFAULT NULL,\n  `changes` json DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `entity_id` (`entity_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;", alters[1].SQL)
	assert.Equal(t, "CREATE TABLE `test`.`_log_default_schemaEntityRef` (\n  `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,\n  `entity_id` int(10) unsigned NOT NULL,\n  `added_at` datetime NOT NULL,\n  `meta` json DEFAULT NULL,\n  `before` json DEFAULT NULL,\n  `changes` json DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `entity_id` (`entity_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;", alters[2].SQL)
	assert.Equal(t, "CREATE TABLE `test`.`schemaEntity` (\n  `ID` int(10) unsigned NOT NULL AUTO_INCREMENT,\n  `Name` varchar(255) NOT NULL DEFAULT '',\n  `Year` year(4) DEFAULT NULL,\n  `Uint8` tinyint(3) unsigned NOT NULL DEFAULT '0',\n  `Uint16` smallint(5) unsigned NOT NULL DEFAULT '0',\n  `Uint32` int(10) unsigned NOT NULL DEFAULT '0',\n  `Uint32Medium` mediumint(8) unsigned NOT NULL DEFAULT '0',\n  `YearRequired` year(4) NOT NULL DEFAULT '0000',\n  `Uint64` bigint(20) unsigned NOT NULL DEFAULT '0',\n  `Int8` tinyint(4) NOT NULL DEFAULT '0',\n  `Int16` smallint(6) NOT NULL DEFAULT '0',\n  `Int32` int(11) NOT NULL DEFAULT '0',\n  `Int32Medium` mediumint(9) NOT NULL DEFAULT '0',\n  `Int64` bigint(20) NOT NULL DEFAULT '0',\n  `Int` int(11) NOT NULL DEFAULT '0',\n  `Bool` tinyint(1) NOT NULL DEFAULT '0',\n  `Interface` mediumtext,\n  `Float32` float unsigned NOT NULL DEFAULT '0',\n  `Float64` double unsigned NOT NULL DEFAULT '0',\n  `Time` date NOT NULL DEFAULT '0001-01-01',\n  `TimeFull` datetime NOT NULL,\n  `TimeNull` date DEFAULT NULL,\n  `Blob` blob,\n  `MediumBlob` mediumblob,\n  `LongBlob` longblob,\n  `SubStructName` varchar(255) DEFAULT NULL,\n  `SubStructAge` smallint(5) unsigned NOT NULL DEFAULT '0',\n  `NameTranslated` json DEFAULT NULL,\n  `RefOne` int(10) unsigned DEFAULT NULL,\n  `RefOneCascade` int(10) unsigned DEFAULT NULL,\n  `Decimal` decimal(10,2) unsigned NOT NULL DEFAULT '0.00',\n  `Enum` enum('a','b','c') NOT NULL DEFAULT 'a',\n  `Set` set('a','b','c') NOT NULL DEFAULT 'a',\n  `FakeDelete` int(10) unsigned NOT NULL DEFAULT '0',\n  INDEX `RefOneCascade` (`RefOneCascade`),\n  INDEX `RefOne` (`RefOne`),\n  INDEX `TestIndex` (`Name`,`Uint16`),\n  UNIQUE INDEX `TestUniqueIndex` (`Int8`),\n  PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8;", alters[3].SQL)
	assert.Equal(t, "ALTER TABLE `test`.`schemaEntity`\n  ADD CONSTRAINT `test:schemaEntity:RefOneCascade` FOREIGN KEY (`RefOneCascade`) REFERENCES `test`.`schemaEntityRef` (`ID`) ON DELETE CASCADE,\n  ADD CONSTRAINT `test:schemaEntity:RefOne` FOREIGN KEY (`RefOne`) REFERENCES `test`.`schemaEntityRef` (`ID`) ON DELETE RESTRICT;", alters[4].SQL)

	for _, alter := range alters {
		engineDrop.GetMysql(alter.Pool).Exec(alter.SQL)
	}

	engine.GetMysql().Exec("ALTER TABLE `schemaEntity` DROP COLUMN `Name`")
	alters = engine.GetAlters()
	assert.Len(t, alters, 1)
	engine.GetMysql()
	assert.True(t, alters[0].Safe)
	assert.Equal(t, "ALTER TABLE `test`.`schemaEntity`\n    ADD COLUMN `Name` varchar(255) NOT NULL DEFAULT '' AFTER `ID`,\n    CHANGE COLUMN `Year` `Year` year(4) DEFAULT NULL AFTER `Name`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint8` `Uint8` tinyint(3) unsigned NOT NULL DEFAULT '0' AFTER `Year`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint16` `Uint16` smallint(5) unsigned NOT NULL DEFAULT '0' AFTER `Uint8`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint32` `Uint32` int(10) unsigned NOT NULL DEFAULT '0' AFTER `Uint16`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint32Medium` `Uint32Medium` mediumint(8) unsigned NOT NULL DEFAULT '0' AFTER `Uint32`,/*CHANGED ORDER*/\n    CHANGE COLUMN `YearRequired` `YearRequired` year(4) NOT NULL DEFAULT '0000' AFTER `Uint32Medium`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint64` `Uint64` bigint(20) unsigned NOT NULL DEFAULT '0' AFTER `YearRequired`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int8` `Int8` tinyint(4) NOT NULL DEFAULT '0' AFTER `Uint64`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int16` `Int16` smallint(6) NOT NULL DEFAULT '0' AFTER `Int8`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int32` `Int32` int(11) NOT NULL DEFAULT '0' AFTER `Int16`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int32Medium` `Int32Medium` mediumint(9) NOT NULL DEFAULT '0' AFTER `Int32`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int64` `Int64` bigint(20) NOT NULL DEFAULT '0' AFTER `Int32Medium`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int` `Int` int(11) NOT NULL DEFAULT '0' AFTER `Int64`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Bool` `Bool` tinyint(1) NOT NULL DEFAULT '0' AFTER `Int`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Interface` `Interface` mediumtext AFTER `Bool`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Float32` `Float32` float unsigned NOT NULL DEFAULT '0' AFTER `Interface`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Float64` `Float64` double unsigned NOT NULL DEFAULT '0' AFTER `Float32`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Time` `Time` date NOT NULL DEFAULT '0001-01-01' AFTER `Float64`,/*CHANGED ORDER*/\n    CHANGE COLUMN `TimeFull` `TimeFull` datetime NOT NULL AFTER `Time`,/*CHANGED ORDER*/\n    CHANGE COLUMN `TimeNull` `TimeNull` date DEFAULT NULL AFTER `TimeFull`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Blob` `Blob` blob AFTER `TimeNull`,/*CHANGED ORDER*/\n    CHANGE COLUMN `MediumBlob` `MediumBlob` mediumblob AFTER `Blob`,/*CHANGED ORDER*/\n    CHANGE COLUMN `LongBlob` `LongBlob` longblob AFTER `MediumBlob`,/*CHANGED ORDER*/\n    CHANGE COLUMN `SubStructName` `SubStructName` varchar(255) DEFAULT NULL AFTER `LongBlob`,/*CHANGED ORDER*/\n    CHANGE COLUMN `SubStructAge` `SubStructAge` smallint(5) unsigned NOT NULL DEFAULT '0' AFTER `SubStructName`,/*CHANGED ORDER*/\n    CHANGE COLUMN `NameTranslated` `NameTranslated` json DEFAULT NULL AFTER `SubStructAge`,/*CHANGED ORDER*/\n    CHANGE COLUMN `RefOne` `RefOne` int(10) unsigned DEFAULT NULL AFTER `NameTranslated`,/*CHANGED ORDER*/\n    CHANGE COLUMN `RefOneCascade` `RefOneCascade` int(10) unsigned DEFAULT NULL AFTER `RefOne`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Decimal` `Decimal` decimal(10,2) unsigned NOT NULL DEFAULT '0.00' AFTER `RefOneCascade`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Enum` `Enum` enum('a','b','c') NOT NULL DEFAULT 'a' AFTER `Decimal`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Set` `Set` set('a','b','c') NOT NULL DEFAULT 'a' AFTER `Enum`,/*CHANGED ORDER*/\n    CHANGE COLUMN `FakeDelete` `FakeDelete` int(10) unsigned NOT NULL DEFAULT '0' AFTER `Set`,/*CHANGED ORDER*/\n    DROP INDEX `TestIndex`,\n    ADD INDEX `TestIndex` (`Name`,`Uint16`);", alters[0].SQL)
}
