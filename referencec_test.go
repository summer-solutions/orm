package orm

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type testEntityReferenceLevel1 struct {
	ORM            `orm:"localCache"`
	ID             uint
	ReferenceOne   *testEntityReferenceLevel2 `orm:"required"`
	ReferenceFive  *testEntityReferenceLevel3
	ReferenceSix   *testEntityReferenceLevel3
	ReferenceSeven *testEntityReferenceLevel4
}

type testEntityReferenceLevel2 struct {
	ORM          `orm:"localCache"`
	ID           uint
	Name         string
	ReferenceTwo *testEntityReferenceLevel3 `orm:"required"`
}

type testEntityReferenceLevel3 struct {
	ORM            `orm:"localCache"`
	ID             uint
	Name           string
	ReferenceThree *testEntityReferenceLevel4 `orm:"required"`
}

type testEntityReferenceLevel4 struct {
	ORM  `orm:"localCache"`
	ID   uint
	Name string
}

func TestReferences(t *testing.T) {
	ref1 := testEntityReferenceLevel1{}
	ref2 := testEntityReferenceLevel2{Name: "name 2"}
	ref3 := testEntityReferenceLevel3{Name: "name 3"}
	ref3b := testEntityReferenceLevel3{Name: "name 3b"}
	ref4 := testEntityReferenceLevel4{Name: "name 4"}
	ref4b := testEntityReferenceLevel4{Name: "name 4b"}

	engine := PrepareTables(t, &Registry{}, ref1, ref2, ref3, ref4)
	engine.Track(&ref1, &ref2, &ref3, &ref4, &ref3b, &ref4b)
	ref1.ReferenceOne = &ref2
	ref1.ReferenceSix = &ref3b
	ref1.ReferenceSeven = &ref4b
	ref2.ReferenceTwo = &ref3
	ref3.ReferenceThree = &ref4
	ref3b.ReferenceThree = &ref4

	err := engine.Flush()
	assert.Nil(t, err)

	assert.Equal(t, uint(1), ref1.ID)
	assert.Equal(t, uint(1), ref1.ReferenceOne.ID)
	assert.Nil(t, ref1.ReferenceFive)
	assert.Equal(t, uint(2), ref1.ReferenceSix.ID)
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

	var root testEntityReferenceLevel1
	has, err := engine.LoadByID(1, &root, "ReferenceOne/ReferenceTwo/ReferenceThree", "ReferenceFive", "ReferenceSix/*")
	assert.Nil(t, err)
	assert.True(t, has)

	assert.Equal(t, "name 2", root.ReferenceOne.Name)
	assert.Equal(t, "name 3", root.ReferenceOne.ReferenceTwo.Name)
	assert.Equal(t, "", root.ReferenceSeven.Name)
	assert.Equal(t, "name 4", root.ReferenceOne.ReferenceTwo.ReferenceThree.Name)
	assert.Equal(t, "name 3b", root.ReferenceSix.Name)
	assert.Equal(t, "name 4", root.ReferenceSix.ReferenceThree.Name)

	has, err = engine.LoadByID(1, &root)
	assert.Nil(t, err)
	assert.True(t, has)
	assert.False(t, engine.Loaded(root.ReferenceOne))
	err = engine.Load(root.ReferenceOne)
	assert.Nil(t, err)
	assert.True(t, engine.Loaded(root.ReferenceOne))

	engine.Track(&root)
	root.ReferenceFive = &testEntityReferenceLevel3{ID: 2}
	assert.False(t, engine.Loaded(root.ReferenceFive))
	err = engine.Load(root.ReferenceFive)
	assert.Nil(t, err)

	err = engine.Flush()
	assert.Nil(t, err)
	has, err = engine.LoadByID(1, &root)
	assert.Nil(t, err)
	assert.True(t, has)
	assert.Equal(t, uint(2), root.ReferenceFive.ID)
}
