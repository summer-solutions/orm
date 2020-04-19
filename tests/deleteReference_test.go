package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

type TestEntityDeleteReference struct {
	orm.ORM `orm:"localCache"`
	ID      uint
}

type TestEntityDeleteReferenceRefRestrict struct {
	orm.ORM      `orm:"localCache"`
	ID           uint
	ReferenceOne *TestEntityDeleteReference
}

type TestEntityDeleteReferenceRefCascade struct {
	orm.ORM           `orm:"localCache"`
	ID                uint
	ReferenceOne      *TestEntityDeleteReference `orm:"cascade"`
	IndexReferenceOne *orm.CachedQuery           `query:":ReferenceOne = ?"`
}

func TestDeleteReference(t *testing.T) {
	engine := PrepareTables(t, &orm.Registry{}, TestEntityDeleteReference{},
		TestEntityDeleteReferenceRefRestrict{}, TestEntityDeleteReferenceRefCascade{})
	entity1 := &TestEntityDeleteReference{}
	entity2 := &TestEntityDeleteReference{}
	err := engine.Flush(entity1, entity2)
	assert.Nil(t, err)

	entityRestrict := &TestEntityDeleteReferenceRefRestrict{}
	entityRestrict.Init(entityRestrict, engine)
	entityRestrict.ReferenceOne.ID = 1
	err = engine.Flush(entityRestrict)
	assert.Nil(t, err)

	entity1.MarkToDelete()
	err = engine.Flush(entity1)
	assert.NotNil(t, err)
	assert.IsType(t, &orm.ForeignKeyError{}, err)
	assert.Equal(t, "test:TestEntityDeleteReferenceRefRestrict:ReferenceOne", err.(*orm.ForeignKeyError).Constraint)

	entityCascade := &TestEntityDeleteReferenceRefCascade{}
	entityCascade2 := &TestEntityDeleteReferenceRefCascade{}
	entityCascade.Init(entityCascade, engine)
	entityCascade2.Init(entityCascade2, engine)
	entityCascade.ReferenceOne.ID = 2
	entityCascade2.ReferenceOne.ID = 2
	err = engine.Flush(entityCascade, entityCascade2)
	assert.Nil(t, err)

	var rows []*TestEntityDeleteReferenceRefCascade
	total, err := engine.CachedSearch(&rows, "IndexReferenceOne", nil, 2)
	assert.Nil(t, err)
	assert.Equal(t, 2, total)

	entity2.MarkToDelete()
	err = engine.Flush(entity2)
	assert.Nil(t, err)

	total, err = engine.CachedSearch(&rows, "IndexReferenceOne", nil, 2)
	assert.Nil(t, err)
	assert.Equal(t, 0, total)
}
