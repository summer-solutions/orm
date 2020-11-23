package orm

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type schemaSubFields struct {
	Name        string `orm:"required"`
	Age         uint16
	RefInStruct *schemaEntityRef
}

type schemaEntityRef struct {
	ORM  `orm:"log"`
	ID   uint
	Name string `orm:"required"`
}

type schemaInvalidIndexEntity struct {
	ORM  `orm:"log"`
	ID   uint
	Name string `orm:"index=TestIndex:invalid"`
}

type schemaInvalidMaxStringEntity struct {
	ORM  `orm:"log"`
	ID   uint
	Name string `orm:"length=invalid"`
}

type schemaToDropEntity struct {
	ORM `orm:"log"`
	ID  uint
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
	ORM             `orm:"log;unique=TestUniqueGlobal:Year,SubStructAge|TestUniqueGlobal2:Uint32"`
	ID              uint
	Name            string `orm:"index=TestIndex;required"`
	NameNullable    string
	NameMax         string  `orm:"length=max"`
	Year            *uint16 `orm:"year"`
	Uint8           uint8
	Uint16          uint16 `orm:"index=TestIndex:2"`
	Uint32          uint32
	Uint32Medium    uint32 `orm:"mediumint"`
	YearRequired    uint16 `orm:"year"`
	Uint64          uint64
	Int8            int8 `orm:"unique=TestUniqueIndex"`
	Int16           int16
	Int32           int32
	Int32Medium     int32 `orm:"mediumint"`
	Int64           int64
	Int             int
	IntNullable     *int
	Bool            bool
	BoolNullable    *bool
	Interface       interface{}
	Float32         float32
	Float32Nullable *float32
	Float64         float64
	Time            time.Time
	TimeFull        time.Time `orm:"time"`
	TimeNull        *time.Time
	Blob            []uint8
	MediumBlob      []uint8 `orm:"mediumblob"`
	LongBlob        []uint8 `orm:"longblob"`
	SubStruct       schemaSubFields
	CachedQuery     *CachedQuery
	Ignored         string `orm:"ignore"`
	NameTranslated  map[string]string
	RefOne          *schemaEntityRef
	RefOneCascade   *schemaEntityRef `orm:"cascade;unique=TestRefOneCascade"`
	RefMany         []*schemaEntityRef
	Decimal         float32  `orm:"decimal=10,2"`
	Enum            string   `orm:"enum=orm.TestEnum;required"`
	Set             []string `orm:"set=orm.TestEnum;required"`
	FakeDelete      bool
}

func TestSchema(t *testing.T) {
	entity := &schemaEntity{}
	ref := &schemaEntityRef{}
	registry := &Registry{}
	registry.RegisterEnumStruct("orm.TestEnum", TestEnum)
	registry.SetDefaultEncoding("utf8")
	engine := PrepareTables(t, registry, entity, ref)

	engineDrop := PrepareTables(t, &Registry{})
	for _, alter := range engineDrop.GetAlters() {
		engineDrop.GetMysql(alter.Pool).Exec(alter.SQL)
	}

	alters := engine.GetAlters()
	assert.Len(t, alters, 5)
	assert.True(t, alters[0].Safe)
	assert.True(t, alters[1].Safe)
	assert.True(t, alters[2].Safe)
	assert.True(t, alters[3].Safe)
	assert.True(t, alters[4].Safe)
	assert.Equal(t, "default", alters[0].Pool)
	assert.Equal(t, "CREATE TABLE `test`.`schemaEntityRef` (\n  `ID` int(10) unsigned NOT NULL AUTO_INCREMENT,\n  `Name` varchar(255) NOT NULL DEFAULT '',\n  PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8;", alters[0].SQL)
	assert.Equal(t, "CREATE TABLE `test`.`_log_default_schemaEntity` (\n  `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,\n  `entity_id` int(10) unsigned NOT NULL,\n  `added_at` datetime NOT NULL,\n  `meta` json DEFAULT NULL,\n  `before` json DEFAULT NULL,\n  `changes` json DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `entity_id` (`entity_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;", alters[1].SQL)
	assert.Equal(t, "CREATE TABLE `test`.`_log_default_schemaEntityRef` (\n  `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,\n  `entity_id` int(10) unsigned NOT NULL,\n  `added_at` datetime NOT NULL,\n  `meta` json DEFAULT NULL,\n  `before` json DEFAULT NULL,\n  `changes` json DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `entity_id` (`entity_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;", alters[2].SQL)
	assert.Equal(t, "CREATE TABLE `test`.`schemaEntity` (\n  `ID` int(10) unsigned NOT NULL AUTO_INCREMENT,\n  `Name` varchar(255) NOT NULL DEFAULT '',\n  `NameNullable` varchar(255) DEFAULT NULL,\n  `NameMax` mediumtext,\n  `Year` year(4) DEFAULT NULL,\n  `Uint8` tinyint(3) unsigned NOT NULL DEFAULT '0',\n  `Uint16` smallint(5) unsigned NOT NULL DEFAULT '0',\n  `Uint32` int(10) unsigned NOT NULL DEFAULT '0',\n  `Uint32Medium` mediumint(8) unsigned NOT NULL DEFAULT '0',\n  `YearRequired` year(4) NOT NULL DEFAULT '0000',\n  `Uint64` bigint(20) unsigned NOT NULL DEFAULT '0',\n  `Int8` tinyint(4) NOT NULL DEFAULT '0',\n  `Int16` smallint(6) NOT NULL DEFAULT '0',\n  `Int32` int(11) NOT NULL DEFAULT '0',\n  `Int32Medium` mediumint(9) NOT NULL DEFAULT '0',\n  `Int64` bigint(20) NOT NULL DEFAULT '0',\n  `Int` int(11) NOT NULL DEFAULT '0',\n  `IntNullable` int(11) DEFAULT NULL,\n  `Bool` tinyint(1) NOT NULL DEFAULT '0',\n  `BoolNullable` tinyint(1) DEFAULT NULL,\n  `Interface` json DEFAULT NULL,\n  `Float32` float unsigned NOT NULL DEFAULT '0',\n  `Float32Nullable` float unsigned DEFAULT NULL,\n  `Float64` double unsigned NOT NULL DEFAULT '0',\n  `Time` date NOT NULL DEFAULT '0001-01-01',\n  `TimeFull` datetime NOT NULL,\n  `TimeNull` date DEFAULT NULL,\n  `Blob` blob,\n  `MediumBlob` mediumblob,\n  `LongBlob` longblob,\n  `SubStructName` varchar(255) NOT NULL DEFAULT '',\n  `SubStructAge` smallint(5) unsigned NOT NULL DEFAULT '0',\n  `SubStructRefInStruct` int(10) unsigned DEFAULT NULL,\n  `NameTranslated` json DEFAULT NULL,\n  `RefOne` int(10) unsigned DEFAULT NULL,\n  `RefOneCascade` int(10) unsigned DEFAULT NULL,\n  `RefMany` json DEFAULT NULL,\n  `Decimal` decimal(10,2) unsigned NOT NULL DEFAULT '0.00',\n  `Enum` enum('a','b','c') NOT NULL DEFAULT 'a',\n  `Set` set('a','b','c') NOT NULL DEFAULT 'a',\n  `FakeDelete` int(10) unsigned NOT NULL DEFAULT '0',\n  INDEX `RefOne` (`RefOne`),\n  INDEX `SubStructRefInStruct` (`SubStructRefInStruct`),\n  INDEX `TestIndex` (`Name`,`Uint16`),\n  UNIQUE INDEX `TestRefOneCascade` (`RefOneCascade`),\n  UNIQUE INDEX `TestUniqueGlobal2` (`Uint32`),\n  UNIQUE INDEX `TestUniqueGlobal` (`Year`,`SubStructAge`),\n  UNIQUE INDEX `TestUniqueIndex` (`Int8`),\n  PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8;", alters[3].SQL)
	assert.Equal(t, "ALTER TABLE `test`.`schemaEntity`\n  ADD CONSTRAINT `test:schemaEntity:RefOneCascade` FOREIGN KEY (`RefOneCascade`) REFERENCES `test`.`schemaEntityRef` (`ID`) ON DELETE CASCADE,\n  ADD CONSTRAINT `test:schemaEntity:RefOne` FOREIGN KEY (`RefOne`) REFERENCES `test`.`schemaEntityRef` (`ID`) ON DELETE RESTRICT,\n  ADD CONSTRAINT `test:schemaEntity:SubStructRefInStruct` FOREIGN KEY (`SubStructRefInStruct`) REFERENCES `test`.`schemaEntityRef` (`ID`) ON DELETE RESTRICT;", alters[4].SQL)

	for _, alter := range alters {
		engineDrop.GetMysql(alter.Pool).Exec(alter.SQL)
	}

	engine.GetMysql().Exec("ALTER TABLE `schemaEntity` DROP COLUMN `Name`")
	alters = engine.GetAlters()
	assert.Len(t, alters, 1)
	assert.True(t, alters[0].Safe)
	assert.Equal(t, "ALTER TABLE `test`.`schemaEntity`\n    ADD COLUMN `Name` varchar(255) NOT NULL DEFAULT '' AFTER `ID`,\n    CHANGE COLUMN `NameNullable` `NameNullable` varchar(255) DEFAULT NULL AFTER `Name`,/*CHANGED ORDER*/\n    CHANGE COLUMN `NameMax` `NameMax` mediumtext AFTER `NameNullable`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Year` `Year` year(4) DEFAULT NULL AFTER `NameMax`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint8` `Uint8` tinyint(3) unsigned NOT NULL DEFAULT '0' AFTER `Year`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint16` `Uint16` smallint(5) unsigned NOT NULL DEFAULT '0' AFTER `Uint8`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint32` `Uint32` int(10) unsigned NOT NULL DEFAULT '0' AFTER `Uint16`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint32Medium` `Uint32Medium` mediumint(8) unsigned NOT NULL DEFAULT '0' AFTER `Uint32`,/*CHANGED ORDER*/\n    CHANGE COLUMN `YearRequired` `YearRequired` year(4) NOT NULL DEFAULT '0000' AFTER `Uint32Medium`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Uint64` `Uint64` bigint(20) unsigned NOT NULL DEFAULT '0' AFTER `YearRequired`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int8` `Int8` tinyint(4) NOT NULL DEFAULT '0' AFTER `Uint64`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int16` `Int16` smallint(6) NOT NULL DEFAULT '0' AFTER `Int8`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int32` `Int32` int(11) NOT NULL DEFAULT '0' AFTER `Int16`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int32Medium` `Int32Medium` mediumint(9) NOT NULL DEFAULT '0' AFTER `Int32`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int64` `Int64` bigint(20) NOT NULL DEFAULT '0' AFTER `Int32Medium`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Int` `Int` int(11) NOT NULL DEFAULT '0' AFTER `Int64`,/*CHANGED ORDER*/\n    CHANGE COLUMN `IntNullable` `IntNullable` int(11) DEFAULT NULL AFTER `Int`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Bool` `Bool` tinyint(1) NOT NULL DEFAULT '0' AFTER `IntNullable`,/*CHANGED ORDER*/\n    CHANGE COLUMN `BoolNullable` `BoolNullable` tinyint(1) DEFAULT NULL AFTER `Bool`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Interface` `Interface` json DEFAULT NULL AFTER `BoolNullable`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Float32` `Float32` float unsigned NOT NULL DEFAULT '0' AFTER `Interface`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Float32Nullable` `Float32Nullable` float unsigned DEFAULT NULL AFTER `Float32`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Float64` `Float64` double unsigned NOT NULL DEFAULT '0' AFTER `Float32Nullable`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Time` `Time` date NOT NULL DEFAULT '0001-01-01' AFTER `Float64`,/*CHANGED ORDER*/\n    CHANGE COLUMN `TimeFull` `TimeFull` datetime NOT NULL AFTER `Time`,/*CHANGED ORDER*/\n    CHANGE COLUMN `TimeNull` `TimeNull` date DEFAULT NULL AFTER `TimeFull`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Blob` `Blob` blob AFTER `TimeNull`,/*CHANGED ORDER*/\n    CHANGE COLUMN `MediumBlob` `MediumBlob` mediumblob AFTER `Blob`,/*CHANGED ORDER*/\n    CHANGE COLUMN `LongBlob` `LongBlob` longblob AFTER `MediumBlob`,/*CHANGED ORDER*/\n    CHANGE COLUMN `SubStructName` `SubStructName` varchar(255) NOT NULL DEFAULT '' AFTER `LongBlob`,/*CHANGED ORDER*/\n    CHANGE COLUMN `SubStructAge` `SubStructAge` smallint(5) unsigned NOT NULL DEFAULT '0' AFTER `SubStructName`,/*CHANGED ORDER*/\n    CHANGE COLUMN `SubStructRefInStruct` `SubStructRefInStruct` int(10) unsigned DEFAULT NULL AFTER `SubStructAge`,/*CHANGED ORDER*/\n    CHANGE COLUMN `NameTranslated` `NameTranslated` json DEFAULT NULL AFTER `SubStructRefInStruct`,/*CHANGED ORDER*/\n    CHANGE COLUMN `RefOne` `RefOne` int(10) unsigned DEFAULT NULL AFTER `NameTranslated`,/*CHANGED ORDER*/\n    CHANGE COLUMN `RefOneCascade` `RefOneCascade` int(10) unsigned DEFAULT NULL AFTER `RefOne`,/*CHANGED ORDER*/\n    CHANGE COLUMN `RefMany` `RefMany` json DEFAULT NULL AFTER `RefOneCascade`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Decimal` `Decimal` decimal(10,2) unsigned NOT NULL DEFAULT '0.00' AFTER `RefMany`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Enum` `Enum` enum('a','b','c') NOT NULL DEFAULT 'a' AFTER `Decimal`,/*CHANGED ORDER*/\n    CHANGE COLUMN `Set` `Set` set('a','b','c') NOT NULL DEFAULT 'a' AFTER `Enum`,/*CHANGED ORDER*/\n    CHANGE COLUMN `FakeDelete` `FakeDelete` int(10) unsigned NOT NULL DEFAULT '0' AFTER `Set`,/*CHANGED ORDER*/\n    DROP INDEX `TestIndex`,\n    ADD INDEX `TestIndex` (`Name`,`Uint16`);", alters[0].SQL)
	engine.GetMysql().Exec(alters[0].SQL)

	engine.GetMysql().Exec("ALTER TABLE `_log_default_schemaEntity` DROP COLUMN `meta`")
	alters = engine.GetAlters()
	assert.Len(t, alters, 2)
	assert.True(t, alters[0].Safe)
	assert.Equal(t, "DROP TABLE `test`.`_log_default_schemaEntity`;", alters[0].SQL)
	assert.Equal(t, "CREATE TABLE `test`.`_log_default_schemaEntity` (\n  `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,\n  `entity_id` int(10) unsigned NOT NULL,\n  `added_at` datetime NOT NULL,\n  `meta` json DEFAULT NULL,\n  `before` json DEFAULT NULL,\n  `changes` json DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `entity_id` (`entity_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;", alters[1].SQL)
	engine.GetMysql().Exec(alters[0].SQL)
	engine.GetMysql().Exec(alters[1].SQL)

	engine.GetMysql().Exec("ALTER TABLE `schemaEntity` CHANGE COLUMN `Year` `Year` varchar(255) NOT NULL DEFAULT ''")
	alters = engine.GetAlters()
	assert.Len(t, alters, 1)
	assert.Equal(t, "ALTER TABLE `test`.`schemaEntity`\n    CHANGE COLUMN `Year` `Year` year(4) DEFAULT NULL AFTER `NameMax`;/*CHANGED FROM `Year` varchar(255) NOT NULL DEFAULT ''*/", alters[0].SQL)
	engine.GetMysql().Exec(alters[0].SQL)

	engine.GetMysql().Exec("ALTER TABLE `schemaEntity` ADD COLUMN `Year2` varchar(255) NOT NULL DEFAULT ''")
	alters = engine.GetAlters()
	assert.Len(t, alters, 1)
	assert.Equal(t, "ALTER TABLE `test`.`schemaEntity`\n    DROP COLUMN `Year2`;", alters[0].SQL)
	engine.GetMysql().Exec(alters[0].SQL)

	engine.GetMysql().Exec("ALTER TABLE `schemaEntity` DROP INDEX `TestIndex`")
	alters = engine.GetAlters()
	assert.Len(t, alters, 1)
	assert.Equal(t, "ALTER TABLE `test`.`schemaEntity`\n    ADD INDEX `TestIndex` (`Name`,`Uint16`);", alters[0].SQL)
	engine.GetMysql().Exec(alters[0].SQL)

	engine.GetMysql().Exec("ALTER TABLE `schemaEntity` DROP FOREIGN KEY `test:schemaEntity:RefOne`")
	alters = engine.GetAlters()
	assert.Len(t, alters, 1)
	assert.Equal(t, "ALTER TABLE `test`.`schemaEntity`\n    ADD CONSTRAINT `test:schemaEntity:RefOne` FOREIGN KEY (`RefOne`) REFERENCES `test`.`schemaEntityRef` (`ID`) ON DELETE RESTRICT;", alters[0].SQL)
	engine.GetMysql().Exec(alters[0].SQL)

	engine.GetMysql().Exec("ALTER TABLE `schemaEntity` DROP FOREIGN KEY `test:schemaEntity:RefOne`")
	engine.GetMysql().Exec("ALTER TABLE `test`.`schemaEntity` ADD CONSTRAINT `test:schemaEntity:RefOne` FOREIGN KEY (`RefOne`) REFERENCES `test`.`schemaEntityRef` (`ID`) ON DELETE CASCADE;")
	alters = engine.GetAlters()
	assert.Len(t, alters, 2)
	assert.Equal(t, "ALTER TABLE `test`.`schemaEntity`\n    DROP FOREIGN KEY `test:schemaEntity:RefOne`;", alters[0].SQL)
	assert.Equal(t, "ALTER TABLE `test`.`schemaEntity`\n    ADD CONSTRAINT `test:schemaEntity:RefOne` FOREIGN KEY (`RefOne`) REFERENCES `test`.`schemaEntityRef` (`ID`) ON DELETE RESTRICT;", alters[1].SQL)
	engine.GetMysql().Exec(alters[0].SQL)
	engine.GetMysql().Exec(alters[1].SQL)

	schema := engine.GetRegistry().GetTableSchemaForEntity(entity)
	assert.Equal(t, "orm.schemaEntity", schema.GetType().String())
	references := schema.GetReferences()
	assert.Len(t, references, 3)
	columns := schema.GetColumns()
	assert.Len(t, columns, 41)

	engine.GetMysql().Exec("ALTER TABLE `schemaEntity` ADD INDEX `TestIndex2` (`Name`);")
	alters = engine.GetAlters()
	assert.Len(t, alters, 1)
	assert.Equal(t, "ALTER TABLE `test`.`schemaEntity`\n    DROP INDEX `TestIndex2`;", alters[0].SQL)
	schema.UpdateSchema(engine)

	engine.GetMysql().Exec("ALTER TABLE `test`.`schemaEntity` ADD CONSTRAINT `test:schemaEntity:RefOne2` FOREIGN KEY (`RefOne`) REFERENCES `test`.`schemaEntityRef` (`ID`) ON DELETE CASCADE;")
	alters = engine.GetAlters()
	assert.Len(t, alters, 1)
	assert.Equal(t, "ALTER TABLE `test`.`schemaEntity`\n    DROP FOREIGN KEY `test:schemaEntity:RefOne2`;", alters[0].SQL)
	engine.GetMysql().Exec(alters[0].SQL)
	alters = engine.GetAlters()
	assert.Len(t, alters, 0)

	engine.TrackAndFlush(&schemaEntityRef{Name: "test"})
	engine.GetMysql().Exec("ALTER TABLE `schemaEntityRef` ADD COLUMN `Year2` varchar(255) NOT NULL DEFAULT ''")
	alters = engine.GetAlters()
	assert.Len(t, alters, 1)
	assert.False(t, alters[0].Safe)
	assert.Equal(t, "ALTER TABLE `test`.`schemaEntityRef`\n    DROP COLUMN `Year2`;", alters[0].SQL)
	engine.GetRegistry().GetTableSchemaForEntity(&schemaEntityRef{}).UpdateSchemaAndTruncateTable(engine)
	alters = engine.GetAlters()
	assert.Len(t, alters, 0)

	registry = &Registry{}
	registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test")
	registry.RegisterEntity(&schemaInvalidIndexEntity{})
	_, err := registry.Validate()
	assert.EqualError(t, err, "invalid entity struct 'orm.schemaInvalidIndexEntity': invalid index position 'invalid' in index 'TestIndex'")

	registry = &Registry{}
	registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test")
	registry.RegisterEntity(&schemaInvalidMaxStringEntity{})
	_, err = registry.Validate()
	assert.EqualError(t, err, "invalid entity struct 'orm.schemaInvalidMaxStringEntity': invalid max string: invalid")

	registry = &Registry{}
	registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test")
	registry.RegisterEntity(&schemaEntity{})
	_, err = registry.Validate()
	assert.EqualError(t, err, "invalid entity struct 'orm.schemaEntity': unregistered enum orm.TestEnum")

	engine = PrepareTables(t, &Registry{}, &schemaToDropEntity{})
	schema = engine.GetRegistry().GetTableSchemaForEntity(&schemaToDropEntity{})
	schema.DropTable(engine)
	has, alters := schema.GetSchemaChanges(engine)
	assert.True(t, has)
	assert.Len(t, alters, 1)
	assert.Equal(t, "CREATE TABLE `test`.`schemaToDropEntity` (\n  `ID` int(10) unsigned NOT NULL AUTO_INCREMENT,\n  PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;", alters[0].SQL)

	registry = &Registry{}
	registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test")
	type invalidSchema struct {
		ORM `orm:"mysql=invalid"`
		ID  uint
	}
	registry.RegisterEntity(&invalidSchema{})
	_, err = registry.Validate()
	assert.EqualError(t, err, "mysql pool 'invalid' not found")

	registry = &Registry{}
	registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test")
	type invalidSchema2 struct {
		ORM `orm:"localCache=invalid"`
		ID  uint
	}
	registry.RegisterEntity(&invalidSchema2{})
	_, err = registry.Validate()
	assert.EqualError(t, err, "local cache pool 'invalid' not found")

	registry = &Registry{}
	registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test")
	type invalidSchema3 struct {
		ORM `orm:"redisCache=invalid"`
		ID  uint
	}
	registry.RegisterEntity(&invalidSchema3{})
	_, err = registry.Validate()
	assert.EqualError(t, err, "redis pool 'invalid' not found")

	registry = &Registry{}
	registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test", "other")
	type invalidSchema4 struct {
		ORM `orm:"mysql=other"`
		ID  uint
	}
	registry.RegisterEntity(&invalidSchema4{})
	_, err = registry.Validate()
	assert.NoError(t, err)

	registry = &Registry{}
	registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test")
	type invalidSchema5 struct {
		ORM
		ID   uint
		Name string `orm:"index=test,test2"`
	}
	registry.RegisterEntity(&invalidSchema5{})
	_, err = registry.Validate()
	assert.NotNil(t, err)

	registry = &Registry{}
	registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test")
	type invalidSchema6 struct {
		ORM
		ID        uint
		Name      string
		IndexName *CachedQuery `queryOne:":Name = ?"`
	}
	registry.RegisterEntity(&invalidSchema6{})
	_, err = registry.Validate()
	assert.EqualError(t, err, "missing unique index for cached query 'IndexName' in orm.invalidSchema6")

	registry = &Registry{}
	registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test")
	type invalidSchema7 struct {
		ORM
		ID        uint
		Name      string
		IndexName *CachedQuery `query:":Name = ?"`
	}
	registry.RegisterEntity(&invalidSchema7{})
	_, err = registry.Validate()
	assert.EqualError(t, err, "missing index for cached query 'IndexName' in orm.invalidSchema7")

	registry = &Registry{}
	registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test")
	type invalidSchema8 struct {
		ORM
		ID        uint
		Name      string       `orm:"unique=TestUniqueIndex"`
		Age       uint         `orm:"unique=TestUniqueIndex:2"`
		IndexName *CachedQuery `queryOne:":Name = ?"`
	}
	registry.RegisterEntity(&invalidSchema8{})
	_, err = registry.Validate()
	assert.EqualError(t, err, "missing unique index for cached query 'IndexName' in orm.invalidSchema8")

	registry = &Registry{}
	registry.RegisterMySQLPool("root:root@tcp(localhost:3311)/test")
	type invalidSchema9 struct {
		ORM
		ID        uint
		Name      string `orm:"index=TestIndex"`
		Age       uint
		IndexName *CachedQuery `query:":Name = ? ORDER BY :Age DESC"`
	}
	registry.RegisterEntity(&invalidSchema9{})
	_, err = registry.Validate()
	assert.EqualError(t, err, "missing index for cached query 'IndexName' in orm.invalidSchema9")
}
