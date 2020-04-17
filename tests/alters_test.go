package tests

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

type AddressSchema struct {
	Street   string
	Building uint16
}

type fieldsColors struct {
	Red    string
	Green  string
	Blue   string
	Yellow string
	Purple string
}

var Color = &fieldsColors{
	Red:    "Red",
	Green:  "Green",
	Blue:   "Blue",
	Yellow: "Yellow",
	Purple: "Purple",
}

type TestEntitySchema struct {
	orm.ORM              `orm:"mysql=schema"`
	ID                   uint
	Name                 string `orm:"length=100;index=FirstIndex"`
	NameNotNull          string `orm:"length=100;index=FirstIndex;required"`
	BigName              string `orm:"length=max"`
	Uint8                uint8  `orm:"unique=SecondIndex:2,ThirdIndex"`
	Uint24               uint32 `orm:"mediumint=true"`
	Uint32               uint32
	Uint64               uint64 `orm:"unique=SecondIndex"`
	Int8                 int8
	Int16                int16
	Int32                int32
	Int32Medium          int32 `orm:"mediumint=true"`
	Int64                int64
	Rune                 rune
	Int                  int
	Bool                 bool
	Float32              float32
	Float64              float64
	Float32Decimal       float32  `orm:"decimal=8,2"`
	Float64DecimalSigned float64  `orm:"decimal=8,2;unsigned=false"`
	Enum                 string   `orm:"enum=tests.Color"`
	EnumNotNull          string   `orm:"enum=tests.Color;required"`
	Set                  []string `orm:"set=tests.Color"`
	Year                 uint16   `orm:"year=true"`
	YearNotNull          uint16   `orm:"year=true;required"`
	Date                 *time.Time
	DateNotNull          time.Time
	DateTime             *time.Time `orm:"time=true"`
	Address              AddressSchema
	JSON                 interface{}
	ReferenceOne         *TestEntitySchemaRef
	ReferenceOneCascade  *TestEntitySchemaRef `orm:"cascade"`
	IgnoreField          []time.Time          `orm:"ignore"`
	Blob                 []byte
	IndexAll             *orm.CachedQuery `query:"" orm:"max=100"`
}

type TestEntitySchemaRef struct {
	orm.ORM `orm:"mysql=schema"`
	ID      uint
	Name    string
}

func TestGetAlters(t *testing.T) {
	registry := &orm.Registry{}
	registry.RegisterMySQLPool("root:root@tcp(localhost:3308)/test_schema", "schema")

	var entity TestEntitySchema
	var entityRef TestEntitySchemaRef
	registry.RegisterEntity(entity, entityRef)
	registry.RegisterEnum("tests.Color", Color)

	config, err := registry.CreateConfig()
	assert.Nil(t, err)
	engine := orm.NewEngine(config)

	tableSchema, _ := config.GetTableSchema("tests.TestEntitySchema")
	err = tableSchema.DropTable(engine)
	assert.Nil(t, err)
	tableSchemaRef, _ := config.GetTableSchema(entityRef)
	err = tableSchemaRef.DropTable(engine)
	assert.Nil(t, err)
	_, err = tableSchemaRef.GetMysql(engine).Exec("DROP TABLE IF EXISTS `ToDrop`")
	assert.Nil(t, err)

	alters, err := engine.GetAlters()
	assert.Nil(t, err)
	assert.Len(t, alters, 3)

	assert.True(t, alters[0].Safe)
	assert.True(t, alters[1].Safe)
	assert.True(t, alters[2].Safe)

	assert.Equal(t, "CREATE TABLE `test_schema`.`TestEntitySchemaRef` (\n  `ID` int(10) unsigned NOT NULL AUTO_INCREMENT,\n  `Name` varchar(255) DEFAULT NULL,\n  PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8;", alters[0].SQL)
	assert.Equal(t, "CREATE TABLE `test_schema`.`TestEntitySchema` (\n  `ID` int(10) unsigned NOT NULL AUTO_INCREMENT,\n  `Name` varchar(100) DEFAULT NULL,\n  `NameNotNull` varchar(100) NOT NULL DEFAULT '',\n  `BigName` mediumtext,\n  `Uint8` tinyint(3) unsigned NOT NULL DEFAULT '0',\n  `Uint24` mediumint(8) unsigned NOT NULL DEFAULT '0',\n  `Uint32` int(10) unsigned NOT NULL DEFAULT '0',\n  `Uint64` bigint(20) unsigned NOT NULL DEFAULT '0',\n  `Int8` tinyint(4) NOT NULL DEFAULT '0',\n  `Int16` smallint(6) NOT NULL DEFAULT '0',\n  `Int32` int(11) NOT NULL DEFAULT '0',\n  `Int32Medium` mediumint(9) NOT NULL DEFAULT '0',\n  `Int64` bigint(20) NOT NULL DEFAULT '0',\n  `Rune` int(11) NOT NULL DEFAULT '0',\n  `Int` int(11) NOT NULL DEFAULT '0',\n  `Bool` tinyint(1) NOT NULL DEFAULT '0',\n  `Float32` float unsigned NOT NULL DEFAULT '0',\n  `Float64` double unsigned NOT NULL DEFAULT '0',\n  `Float32Decimal` decimal(8,2) unsigned NOT NULL DEFAULT '0.00',\n  `Float64DecimalSigned` decimal(8,2) NOT NULL DEFAULT '0.00',\n  `Enum` enum('Red','Green','Blue','Yellow','Purple') DEFAULT NULL,\n  `EnumNotNull` enum('Red','Green','Blue','Yellow','Purple') NOT NULL DEFAULT 'Red',\n  `Set` set('Red','Green','Blue','Yellow','Purple') DEFAULT NULL,\n  `Year` year(4) DEFAULT NULL,\n  `YearNotNull` year(4) NOT NULL DEFAULT '0000',\n  `Date` date DEFAULT NULL,\n  `DateNotNull` date NOT NULL DEFAULT '0001-01-01',\n  `DateTime` datetime DEFAULT NULL,\n  `AddressStreet` varchar(255) DEFAULT NULL,\n  `AddressBuilding` smallint(5) unsigned NOT NULL DEFAULT '0',\n  `JSON` mediumtext,\n  `ReferenceOne` int(10) unsigned DEFAULT NULL,\n  `ReferenceOneCascade` int(10) unsigned DEFAULT NULL,\n  `Blob` blob,\n  INDEX `FirstIndex` (`NameNotNull`),\n  INDEX `ReferenceOneCascade` (`ReferenceOneCascade`),\n  INDEX `ReferenceOne` (`ReferenceOne`),\n  UNIQUE INDEX `SecondIndex` (`Uint64`,`Uint8`),\n  UNIQUE INDEX `ThirdIndex` (`Uint8`),\n  PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8;", alters[1].SQL)
	assert.Equal(t, "ALTER TABLE `test_schema`.`TestEntitySchema`\n  ADD CONSTRAINT `test_schema:TestEntitySchema:ReferenceOneCascade` FOREIGN KEY (`ReferenceOneCascade`) REFERENCES `test_schema`.`TestEntitySchemaRef` (`ID`) ON DELETE CASCADE,\n  ADD CONSTRAINT `test_schema:TestEntitySchema:ReferenceOne` FOREIGN KEY (`ReferenceOne`) REFERENCES `test_schema`.`TestEntitySchemaRef` (`ID`) ON DELETE RESTRICT;", alters[2].SQL)

	for _, alter := range alters {
		schema, has := config.GetTableSchema(entity)
		assert.True(t, has)
		_, err = schema.GetMysql(engine).Exec(alter.SQL)
		assert.Nil(t, err)
	}

	schema, has := config.GetTableSchema(entity)
	assert.True(t, has)
	assert.Equal(t, reflect.TypeOf(entity), schema.GetType())
	_, err = schema.GetMysql(engine).Exec("ALTER TABLE `TestEntitySchema` DROP COLUMN `BigName`")
	assert.Nil(t, err)

	alters, err = engine.GetAlters()
	assert.Nil(t, err)
	assert.Len(t, alters, 1)
	assert.True(t, alters[0].Safe)
	assert.Equal(t, "ALTER TABLE `test_schema`.`TestEntitySchema`\n    ADD COLUMN `BigName` mediumtext AFTER `NameNotNull`,\n    CHANGE COLUMN `Uint8` `Uint8` tinyint(3) unsigned NOT NULL DEFAULT '0' AFTER `BigName`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint24` `Uint24` mediumint(8) unsigned NOT NULL DEFAULT '0' AFTER `Uint8`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint32` `Uint32` int(10) unsigned NOT NULL DEFAULT '0' AFTER `Uint24`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint64` `Uint64` bigint(20) unsigned NOT NULL DEFAULT '0' AFTER `Uint32`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int8` `Int8` tinyint(4) NOT NULL DEFAULT '0' AFTER `Uint64`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int16` `Int16` smallint(6) NOT NULL DEFAULT '0' AFTER `Int8`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int32` `Int32` int(11) NOT NULL DEFAULT '0' AFTER `Int16`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int32Medium` `Int32Medium` mediumint(9) NOT NULL DEFAULT '0' AFTER `Int32`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int64` `Int64` bigint(20) NOT NULL DEFAULT '0' AFTER `Int32Medium`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Rune` `Rune` int(11) NOT NULL DEFAULT '0' AFTER `Int64`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int` `Int` int(11) NOT NULL DEFAULT '0' AFTER `Rune`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Bool` `Bool` tinyint(1) NOT NULL DEFAULT '0' AFTER `Int`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Float32` `Float32` float unsigned NOT NULL DEFAULT '0' AFTER `Bool`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Float64` `Float64` double unsigned NOT NULL DEFAULT '0' AFTER `Float32`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Float32Decimal` `Float32Decimal` decimal(8,2) unsigned NOT NULL DEFAULT '0.00' AFTER `Float64`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Float64DecimalSigned` `Float64DecimalSigned` decimal(8,2) NOT NULL DEFAULT '0.00' AFTER `Float32Decimal`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Enum` `Enum` enum('Red','Green','Blue','Yellow','Purple') DEFAULT NULL AFTER `Float64DecimalSigned`,/*CHANGED ORDER*/\n    CHANGE COLUMN `EnumNotNull` `EnumNotNull` enum('Red','Green','Blue','Yellow','Purple') NOT NULL DEFAULT 'Red' AFTER `Enum`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Set` `Set` set('Red','Green','Blue','Yellow','Purple') DEFAULT NULL AFTER `EnumNotNull`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Year` `Year` year(4) DEFAULT NULL AFTER `Set`,/*CHANGED ORDER*/\n    CHANGE COLUMN `YearNotNull` `YearNotNull` year(4) NOT NULL DEFAULT '0000' AFTER `Year`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Date` `Date` date DEFAULT NULL AFTER `YearNotNull`,/*CHANGED ORDER*/\n    CHANGE COLUMN `DateNotNull` `DateNotNull` date NOT NULL DEFAULT '0001-01-01' AFTER `Date`,/*CHANGED ORDER*/\n    CHANGE COLUMN `DateTime` `DateTime` datetime DEFAULT NULL AFTER `DateNotNull`,/*CHANGED ORDER*/\n    CHANGE COLUMN `AddressStreet` `AddressStreet` varchar(255) DEFAULT NULL AFTER `DateTime`,/*CHANGED ORDER*/\n    CHANGE COLUMN `AddressBuilding` `AddressBuilding` smallint(5) unsigned NOT NULL DEFAULT '0' AFTER `AddressStreet`,/*CHANGED ORDER*/\n    CHANGE COLUMN `JSON` `JSON` mediumtext AFTER `AddressBuilding`,/*CHANGED ORDER*/\n    CHANGE COLUMN `ReferenceOne` `ReferenceOne` int(10) unsigned DEFAULT NULL AFTER `JSON`,/*CHANGED ORDER*/\n    CHANGE COLUMN `ReferenceOneCascade` `ReferenceOneCascade` int(10) unsigned DEFAULT NULL AFTER `ReferenceOne`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Blob` `Blob` blob AFTER `ReferenceOneCascade`;/*CHANGED ORDER*/", alters[0].SQL)

	err = schema.UpdateSchema(engine)
	assert.Nil(t, err)
	alters, err = engine.GetAlters()
	assert.Nil(t, err)
	assert.Len(t, alters, 0)

	_, err = schema.GetMysql(engine).Exec("CREATE TABLE `ToDrop` (`ID` int(10) unsigned NOT NULL) ENGINE=InnoDB DEFAULT CHARSET=utf8;")
	assert.Nil(t, err)
	alters, err = engine.GetAlters()
	assert.Nil(t, err)
	assert.Len(t, alters, 1)
	assert.True(t, alters[0].Safe)
	assert.Equal(t, "DROP TABLE IF EXISTS `test_schema`.`ToDrop`;", alters[0].SQL)

	_, err = schema.GetMysql(engine).Exec("INSERT INTO `ToDrop`(ID) VALUES(1)")
	assert.Nil(t, err)
	alters, err = engine.GetAlters()
	assert.Nil(t, err)
	assert.Len(t, alters, 1)
	assert.False(t, alters[0].Safe)
	assert.Equal(t, "DROP TABLE IF EXISTS `test_schema`.`ToDrop`;", alters[0].SQL)

	_, err = schema.GetMysql(engine).Exec(alters[0].SQL)
	assert.Nil(t, err)

	_, err = schema.GetMysql(engine).Exec("INSERT INTO `TestEntitySchema`(Name) VALUES('test')")
	assert.Nil(t, err)
	err = schema.UpdateSchemaAndTruncateTable(engine)
	assert.Nil(t, err)
	has, err = engine.TryByID(1, &entity)
	assert.Nil(t, err)
	assert.False(t, has)

	references := schema.GetReferences()
	assert.Len(t, references, 2)
	assert.Contains(t, references, "ReferenceOne")
	assert.Contains(t, references, "ReferenceOneCascade")

	columns := schema.GetColumns()
	assert.Len(t, columns, 34)
	assert.Equal(t, map[string]string{"Address.Building": "AddressBuilding", "Address.Street": "AddressStreet", "BigName": "BigName", "Blob": "Blob", "Bool": "Bool", "Date": "Date", "DateNotNull": "DateNotNull", "DateTime": "DateTime", "Enum": "Enum", "EnumNotNull": "EnumNotNull", "Float32": "Float32", "Float32Decimal": "Float32Decimal", "Float64": "Float64", "Float64DecimalSigned": "Float64DecimalSigned", "ID": "ID", "Int": "Int", "Int16": "Int16", "Int32": "Int32", "Int32Medium": "Int32Medium", "Int64": "Int64", "Int8": "Int8", "JSON": "JSON", "Name": "Name", "NameNotNull": "NameNotNull", "ReferenceOne.ID": "ReferenceOne", "ReferenceOneCascade.ID": "ReferenceOneCascade", "Rune": "Rune", "Set": "Set", "Uint24": "Uint24", "Uint32": "Uint32", "Uint64": "Uint64", "Uint8": "Uint8", "Year": "Year", "YearNotNull": "YearNotNull"}, columns)

	_, err = schema.GetMysql(engine).Exec("ALTER TABLE `TestEntitySchema` ADD COLUMN `ToDrop` int(8)")
	assert.Nil(t, err)
	alters, err = engine.GetAlters()
	assert.Nil(t, err)
	assert.Len(t, alters, 1)
	assert.True(t, alters[0].Safe)
	assert.Equal(t, "ALTER TABLE `test_schema`.`TestEntitySchema`\n    DROP COLUMN `ToDrop`;", alters[0].SQL)
	err = schema.UpdateSchema(engine)
	assert.Nil(t, err)

	_, err = schema.GetMysql(engine).Exec("ALTER TABLE `TestEntitySchema` CHANGE COLUMN `Enum` `Enum` enum('Red','Black','Blue','Yellow','Purple') DEFAULT NULL")
	assert.Nil(t, err)
	alters, err = engine.GetAlters()
	assert.Nil(t, err)
	assert.Len(t, alters, 1)
	assert.True(t, alters[0].Safe)
	assert.Equal(t, "ALTER TABLE `test_schema`.`TestEntitySchema`\n    CHANGE COLUMN `Enum` `Enum` enum('Red','Green','Blue','Yellow','Purple') DEFAULT NULL AFTER `Float64DecimalSigned`;/*CHANGED FROM `Enum` enum('Red','Black','Blue','Yellow','Purple') DEFAULT NULL*/", alters[0].SQL)
	err = schema.UpdateSchema(engine)
	assert.Nil(t, err)

	_, err = schema.GetMysql(engine).Exec("ALTER TABLE `TestEntitySchema` DROP INDEX `SecondIndex`")
	assert.Nil(t, err)
	alters, err = engine.GetAlters()
	assert.Nil(t, err)
	assert.Len(t, alters, 1)
	assert.True(t, alters[0].Safe)
	assert.Equal(t, "ALTER TABLE `test_schema`.`TestEntitySchema`\n    ADD UNIQUE INDEX `SecondIndex` (`Uint64`,`Uint8`);", alters[0].SQL)
	err = schema.UpdateSchema(engine)
	assert.Nil(t, err)

	_, err = schema.GetMysql(engine).Exec("ALTER TABLE `TestEntitySchema` DROP INDEX `SecondIndex`, ADD UNIQUE INDEX `SecondIndex` (`Uint8`, `Uint64`);")
	assert.Nil(t, err)
	alters, err = engine.GetAlters()
	assert.Nil(t, err)
	assert.Len(t, alters, 1)
	assert.True(t, alters[0].Safe)
	assert.Equal(t, "ALTER TABLE `test_schema`.`TestEntitySchema`\n    DROP INDEX `SecondIndex`,\n    ADD UNIQUE INDEX `SecondIndex` (`Uint64`,`Uint8`);", alters[0].SQL)
	err = schema.UpdateSchema(engine)
	assert.Nil(t, err)

	_, err = schema.GetMysql(engine).Exec("ALTER TABLE `TestEntitySchema` DROP FOREIGN KEY `test_schema:TestEntitySchema:ReferenceOne`")
	assert.Nil(t, err)
	alters, err = engine.GetAlters()
	assert.Nil(t, err)
	assert.Len(t, alters, 1)
	assert.True(t, alters[0].Safe)
	assert.Equal(t, "ALTER TABLE `test_schema`.`TestEntitySchema`\n    ADD CONSTRAINT `test_schema:TestEntitySchema:ReferenceOne` FOREIGN KEY (`ReferenceOne`) REFERENCES `test_schema`.`TestEntitySchemaRef` (`ID`) ON DELETE RESTRICT;", alters[0].SQL)
	err = schema.UpdateSchema(engine)
	assert.Nil(t, err)

	_, err = schema.GetMysql(engine).Exec("ALTER TABLE `TestEntitySchema` DROP FOREIGN KEY `test_schema:TestEntitySchema:ReferenceOne`")
	assert.Nil(t, err)
	_, err = schema.GetMysql(engine).Exec("ALTER TABLE `TestEntitySchema` ADD CONSTRAINT `test_schema:TestEntitySchema:ReferenceOne` FOREIGN KEY (`ReferenceOne`) REFERENCES `test_schema`.`TestEntitySchemaRef` (`ID`) ON DELETE CASCADE;")
	assert.Nil(t, err)
	alters, err = engine.GetAlters()
	assert.Nil(t, err)
	assert.Len(t, alters, 2)
	assert.True(t, alters[0].Safe)
	assert.Equal(t, "ALTER TABLE `test_schema`.`TestEntitySchema`\n    DROP FOREIGN KEY `test_schema:TestEntitySchema:ReferenceOne`;", alters[0].SQL)
	assert.Equal(t, "ALTER TABLE `test_schema`.`TestEntitySchema`\n    ADD CONSTRAINT `test_schema:TestEntitySchema:ReferenceOne` FOREIGN KEY (`ReferenceOne`) REFERENCES `test_schema`.`TestEntitySchemaRef` (`ID`) ON DELETE RESTRICT;", alters[1].SQL)
	err = schema.UpdateSchema(engine)
	assert.Nil(t, err)

	_, err = schema.GetMysql(engine).Exec("ALTER TABLE `TestEntitySchema` ADD KEY `ToDropIndex` (`Uint8`)")
	assert.Nil(t, err)
	alters, err = engine.GetAlters()
	assert.Nil(t, err)
	assert.Len(t, alters, 1)
	assert.True(t, alters[0].Safe)
	assert.Equal(t, "ALTER TABLE `test_schema`.`TestEntitySchema`\n    DROP INDEX `ToDropIndex`;", alters[0].SQL)
	err = schema.UpdateSchema(engine)
	assert.Nil(t, err)

	_, err = schema.GetMysql(engine).Exec("ALTER TABLE `TestEntitySchema` ADD CONSTRAINT `ToDropConstrait` FOREIGN KEY (`ReferenceOne`) REFERENCES `TestEntitySchemaRef` (`ID`)")
	assert.Nil(t, err)
	alters, err = engine.GetAlters()
	assert.Nil(t, err)
	assert.Len(t, alters, 1)
	assert.True(t, alters[0].Safe)
	assert.Equal(t, "ALTER TABLE `test_schema`.`TestEntitySchema`\n    DROP FOREIGN KEY `ToDropConstrait`;", alters[0].SQL)
	err = schema.UpdateSchema(engine)
	assert.Nil(t, err)
}
