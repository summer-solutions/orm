package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type schemaEntity struct {
	ORM            `orm:"log"`
	ID             uint
	Name           string
	NameTranslated map[string]string
}

func TestSchema(t *testing.T) {
	entity := &schemaEntity{}
	engine := PrepareTables(t, &Registry{}, entity)
	schema := engine.GetRegistry().GetTableSchemaForEntity(entity)
	schema.DropTable(engine)
	engine.GetMysql().Exec("DROP TABLE IF EXISTS `_log_default_schemaEntity`")

	alters := engine.GetAlters()
	assert.Len(t, alters, 2)
	assert.True(t, alters[0].Safe)
	assert.True(t, alters[1].Safe)
	assert.Equal(t, "default", alters[0].Pool)
	assert.Equal(t, "CREATE TABLE `test`.`schemaEntity` (\n  `ID` int(10) unsigned NOT NULL AUTO_INCREMENT,\n  `Name` varchar(255) DEFAULT NULL,\n  `NameTranslated` json DEFAULT NULL,\n  PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8;", alters[0].SQL)
	assert.Equal(t, "CREATE TABLE `test`.`_log_default_schemaEntity` (\n  `id` bigint(11) unsigned NOT NULL AUTO_INCREMENT,\n  `entity_id` int(10) unsigned NOT NULL,\n  `added_at` datetime NOT NULL,\n  `meta` json DEFAULT NULL,\n  `before` json DEFAULT NULL,\n  `changes` json DEFAULT NULL,\n  PRIMARY KEY (`id`),\n  KEY `entity_id` (`entity_id`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8 ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=8;", alters[1].SQL)

	schema.UpdateSchema(engine)
	engine.GetMysql().Exec("ALTER TABLE `schemaEntity` DROP COLUMN `Name`")
	alters = engine.GetAlters()
	assert.Len(t, alters, 2)
	engine.GetMysql()
	assert.True(t, alters[0].Safe)
}
