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
	engine.Track(entity1)
	err := engine.Flush()
	assert.Nil(t, err)
	entity2 := &TestEntityDeleteReference{}
	engine.Track(entity2)
	err = engine.Flush()
	assert.Nil(t, err)

	entityRestrict := &TestEntityDeleteReferenceRefRestrict{}
	engine.Track(entityRestrict)
	entityRestrict.ReferenceOne = &TestEntityDeleteReference{ID: 1}
	err = engine.Flush()
	assert.Nil(t, err)

	engine.MarkToDelete(entity1)
	err = engine.Flush()
	assert.NotNil(t, err)
	assert.IsType(t, &orm.ForeignKeyError{}, err)
	assert.Equal(t, "test:TestEntityDeleteReferenceRefRestrict:ReferenceOne", err.(*orm.ForeignKeyError).Constraint)
	engine.ClearTrackedEntities()

	entityCascade := &TestEntityDeleteReferenceRefCascade{}
	entityCascade2 := &TestEntityDeleteReferenceRefCascade{}
	engine.Track(entityCascade)
	engine.Track(entityCascade2)
	entityCascade.ReferenceOne = &TestEntityDeleteReference{ID: 2}
	entityCascade2.ReferenceOne = &TestEntityDeleteReference{ID: 2}
	err = engine.Flush()
	assert.Nil(t, err)
	var rows []*TestEntityDeleteReferenceRefCascade
	total, err := engine.CachedSearch(&rows, "IndexReferenceOne", nil, 2)
	assert.Nil(t, err)
	assert.Equal(t, 2, total)

	engine.MarkToDelete(entity2)
	err = engine.Flush()
	assert.Nil(t, err)

	total, err = engine.CachedSearch(&rows, "IndexReferenceOne", nil, 2)
	assert.Nil(t, err)
	assert.Equal(t, 0, total)
}
