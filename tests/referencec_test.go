package tests

import (
	"github.com/stretchr/testify/assert"
	"testing"

	"github.com/summer-solutions/orm"
)

type TestEntityReferenceLevel1 struct {
	orm.ORM      `orm:"localCache"`
	ID           uint
	ReferenceOne *TestEntityReferenceLevel2  `orm:"required"`
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
	t.SkipNow()
	ref1 := TestEntityReferenceLevel1{}
	ref2 := TestEntityReferenceLevel2{Name: "name 2"}
	ref3 := TestEntityReferenceLevel3{Name: "name 3"}
	ref4 := TestEntityReferenceLevel4{Name: "name 4"}

	engine := PrepareTables(t, &orm.Registry{}, ref1, ref2, ref3, ref4)
	engine.TrackEntity(&ref1, &ref2, &ref3, &ref4)
	ref1.ReferenceOne = &ref2
	ref2.ReferenceTwo = &ref3
	ref3.ReferenceThree = &ref4

	err := engine.FlushTrackedEntities()
	assert.Nil(t, err)
}
