package orm

import (
	"testing"

	"github.com/juju/errors"

	"github.com/stretchr/testify/assert"
)

type testEntityDeleteReference struct {
	ORM `orm:"localCache"`
	ID  uint
}

type testEntityDeleteReferenceRefRestrict struct {
	ORM          `orm:"localCache"`
	ID           uint
	ReferenceOne *testEntityDeleteReference
}

type testEntityDeleteReferenceRefCascade struct {
	ORM               `orm:"localCache"`
	ID                uint
	ReferenceOne      *testEntityDeleteReference `orm:"cascade"`
	IndexReferenceOne *CachedQuery               `query:":ReferenceOne = ?"`
}

func TestDeleteReference(t *testing.T) {
	engine := PrepareTables(t, &Registry{}, testEntityDeleteReference{},
		testEntityDeleteReferenceRefRestrict{}, testEntityDeleteReferenceRefCascade{})
	entity1 := &testEntityDeleteReference{}
	engine.Track(entity1)
	err := engine.Flush()
	assert.Nil(t, err)
	entity2 := &testEntityDeleteReference{}
	engine.Track(entity2)
	err = engine.Flush()
	assert.Nil(t, err)

	entityRestrict := &testEntityDeleteReferenceRefRestrict{}
	engine.Track(entityRestrict)
	entityRestrict.ReferenceOne = &testEntityDeleteReference{ID: 1}
	err = engine.Flush()
	assert.Nil(t, err)

	engine.MarkToDelete(entity1)
	err = engine.Flush()
	assert.NotNil(t, err)
	assert.IsType(t, &ForeignKeyError{}, errors.Cause(err))
	assert.Equal(t, "test:testEntityDeleteReferenceRefRestrict:ReferenceOne", errors.Cause(err).(*ForeignKeyError).Constraint)
	engine.ClearTrackedEntities()

	entityCascade := &testEntityDeleteReferenceRefCascade{}
	entityCascade2 := &testEntityDeleteReferenceRefCascade{}
	engine.Track(entityCascade)
	engine.Track(entityCascade2)
	entityCascade.ReferenceOne = &testEntityDeleteReference{ID: 2}
	entityCascade2.ReferenceOne = &testEntityDeleteReference{ID: 2}
	err = engine.Flush()
	assert.Nil(t, err)
	var rows []*testEntityDeleteReferenceRefCascade
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
