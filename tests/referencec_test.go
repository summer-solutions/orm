package tests

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/summer-solutions/orm"
)

type TestEntityReferenceLevel1 struct {
	orm.ORM      `orm:"localCache"`
	ID           uint
	ReferenceOne *TestEntityReferenceLevel2 `orm:"required"`
}

type TestEntityReferenceLevel2 struct {
	orm.ORM      `orm:"localCache"`
	ID           uint
	Name         string
	ReferenceTwo *TestEntityReferenceLevel3 `orm:"required"`
}

type TestEntityReferenceLevel3 struct {
	orm.ORM        `orm:"localCache"`
	ID             uint
	Name           string
	ReferenceThree *TestEntityReferenceLevel4 `orm:"required"`
}

type TestEntityReferenceLevel4 struct {
	orm.ORM `orm:"localCache"`
	ID      uint
	Name    string
}

func TestReferences(t *testing.T) {
	ref1 := TestEntityReferenceLevel1{}
	ref2 := TestEntityReferenceLevel2{Name: "name 2"}
	ref3 := TestEntityReferenceLevel3{Name: "name 3"}
	ref3b := TestEntityReferenceLevel3{Name: "name 3b"}
	ref4 := TestEntityReferenceLevel4{Name: "name 4"}

	engine := PrepareTables(t, &orm.Registry{}, ref1, ref2, ref3, ref4)
	engine.TrackEntity(&ref1, &ref2, &ref3, &ref4, &ref3b)
	ref1.ReferenceOne = &ref2
	ref2.ReferenceTwo = &ref3
	ref3.ReferenceThree = &ref4
	ref3b.ReferenceThree = &ref4

	err := engine.FlushTrackedEntities()
	assert.Nil(t, err)

	assert.Equal(t, uint(1), ref1.ID)
	assert.Equal(t, uint(1), ref1.ReferenceOne.ID)
	assert.Equal(t, "name 2", ref1.ReferenceOne.Name)
	assert.Equal(t, uint(1), ref2.ID)
	assert.Equal(t, uint(1), ref2.ReferenceTwo.ID)
	assert.Equal(t, "name 3", ref2.ReferenceTwo.Name)
	assert.Equal(t, uint(1), ref3.ID)
	assert.Equal(t, uint(1), ref3.ReferenceThree.ID)
	assert.Equal(t, uint(2), ref3b.ID)
	assert.Equal(t, uint(1), ref3b.ReferenceThree.ID)
	assert.Equal(t, "name 4", ref3.ReferenceThree.Name)
	assert.Equal(t, "name 4", ref1.ReferenceOne.ReferenceTwo.ReferenceThree.Name)
	assert.Equal(t, "name 4", ref3b.ReferenceThree.Name)
}
