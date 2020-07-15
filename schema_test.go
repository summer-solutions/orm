package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type schemaEntity struct {
	ORM
	ID             uint
	Name           string
	NameTranslated map[string]string
}

func TestSchema(t *testing.T) {
	entity := &schemaEntity{}
	engine := PrepareTables(t, &Registry{}, entity)
	engine.GetRegistry().GetTableSchemaForEntity(entity).DropTable(engine)
	alters := engine.GetAlters()
	assert.Len(t, alters, 1)
	assert.True(t, alters[0].Safe)
	assert.Equal(t, "default", alters[0].Pool)
	assert.Equal(t, "CREATE TABLE `test`.`schemaEntity` (\n  `ID` int(10) unsigned NOT NULL AUTO_INCREMENT,\n  `Name` varchar(255) DEFAULT NULL,\n  `NameTranslated` json DEFAULT NULL,\n  PRIMARY KEY (`ID`)\n) ENGINE=InnoDB DEFAULT CHARSET=utf8;", alters[0].SQL)
}
