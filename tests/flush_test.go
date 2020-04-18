package tests

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

type TestEntityFlush struct {
	orm.ORM
	ID           uint16
	Name         string
	NameNotNull  string `orm:"required"`
	Blob         []byte
	Enum         string   `orm:"enum=tests.Color"`
	EnumNotNull  string   `orm:"enum=tests.Color;required"`
	Year         uint16   `orm:"year=true"`
	YearNotNull  uint16   `orm:"year=true;required"`
	Set          []string `orm:"set=tests.Color;required"`
	Date         *time.Time
	DateNotNull  time.Time `orm:"required"`
	ReferenceOne *TestEntityFlush
	Ignored      []time.Time `orm:"ignore"`
}

type TestEntityErrors struct {
	orm.ORM
	ID           uint16
	Name         string `orm:"unique=NameIndex"`
	ReferenceOne *TestEntityErrors
}

func TestFlush(t *testing.T) {
	registry := &orm.Registry{}
	registry.RegisterEnum("tests.Color", Color)
	var entity TestEntityFlush
	engine := PrepareTables(t, registry, entity)

	var entities = make([]*TestEntityFlush, 10)
	flusher := orm.Flusher{}
	for i := 1; i <= 10; i++ {
		e := TestEntityFlush{Name: "Name " + strconv.Itoa(i), EnumNotNull: Color.Red, Set: []string{Color.Red, Color.Blue}}
		flusher.RegisterEntity(&e)
		entities[i-1] = &e
	}
	_, err := flusher.Flush(engine)
	assert.Nil(t, err)
	for i := 1; i < 10; i++ {
		testEntity := entities[i-1]
		assert.Equal(t, uint16(i), testEntity.ID)
		assert.Equal(t, "Name "+strconv.Itoa(i), testEntity.Name)
		assert.False(t, engine.IsDirty(testEntity))
		assert.Nil(t, testEntity.Date)
		assert.NotNil(t, testEntity.DateNotNull)
	}

	entities[1].Name = "Name 2.1"
	entities[1].ReferenceOne.ID = 7
	entities[7].Name = "Name 8.1"
	now := time.Now()
	entities[7].Date = &now

	assert.Nil(t, err)
	assert.True(t, engine.IsDirty(entities[1]))
	assert.True(t, engine.IsDirty(entities[7]))
	err = engine.Flush(entities[1], entities[7])
	assert.Nil(t, err)

	var edited1 TestEntityFlush
	var edited2 TestEntityFlush
	err = engine.GetByID(2, &edited1)
	assert.Nil(t, err)
	err = engine.GetByID(8, &edited2)
	assert.Nil(t, err)
	assert.Equal(t, "Name 2.1", edited1.Name)
	assert.Equal(t, uint16(7), edited1.ReferenceOne.ID)
	assert.Equal(t, "Name 8.1", edited2.Name)
	assert.Equal(t, uint16(0), edited2.ReferenceOne.ID)
	assert.NotNil(t, edited2.Date)
	assert.Equal(t, now.Format("2006-01-02"), edited2.Date.Format("2006-01-02"))
	assert.False(t, edited1.ReferenceOne.Loaded())
	assert.False(t, edited2.ReferenceOne.Loaded())
	err = edited1.ReferenceOne.Load(engine)
	assert.Nil(t, err)
	assert.True(t, edited1.ReferenceOne.Loaded())
	assert.Equal(t, "Name 7", edited1.ReferenceOne.Name)

	err = edited2.ReferenceOne.Refresh(engine)
	assert.Nil(t, err)
	assert.False(t, edited2.ReferenceOne.Loaded())
	assert.Equal(t, "", edited2.ReferenceOne.Name)

	toDelete := edited2
	edited1.Name = "Name 2.2"
	toDelete.MarkToDelete()
	newEntity := TestEntityFlush{Name: "Name 11", EnumNotNull: Color.Red}
	engine.Init(&newEntity)
	assert.True(t, engine.IsDirty(&edited1))
	assert.Nil(t, err)
	assert.True(t, engine.IsDirty(&edited2))
	assert.Nil(t, err)
	assert.True(t, engine.IsDirty(&newEntity))

	err = engine.Flush(&edited1, &newEntity, &toDelete)
	assert.Nil(t, err)

	assert.False(t, engine.IsDirty(&edited1))
	assert.False(t, engine.IsDirty(&newEntity))

	var edited3 TestEntityFlush
	var deleted TestEntityFlush
	var new11 TestEntityFlush
	err = engine.GetByID(2, &edited3)
	assert.Nil(t, err)
	hasDeleted, err := engine.TryByID(8, &deleted)
	assert.Nil(t, err)
	err = engine.GetByID(11, &new11)
	assert.Nil(t, err)
	assert.Equal(t, "Name 2.2", edited3.Name)
	assert.False(t, hasDeleted)
	assert.Equal(t, "Name 11", new11.Name)
}

func TestFlushErrors(t *testing.T) {
	entity := TestEntityErrors{Name: "Name"}
	engine := PrepareTables(t, &orm.Registry{}, entity)
	engine.Init(&entity)

	entity.ReferenceOne.ID = 2
	err := engine.Flush(&entity)
	assert.NotNil(t, err)
	keyError, is := err.(*orm.ForeignKeyError)
	assert.True(t, is)
	assert.Equal(t, "test:TestEntityErrors:ReferenceOne", keyError.Constraint)
	assert.Equal(t, "Cannot add or update a child row: a foreign key constraint fails (`test`.`TestEntityErrors`, CONSTRAINT `test:TestEntityErrors:ReferenceOne` FOREIGN KEY (`ReferenceOne`) REFERENCES `TestEntityErrors` (`ID`))", keyError.Error())

	entity.ReferenceOne.ID = 0
	err = engine.Flush(&entity)
	assert.Nil(t, err)

	entity = TestEntityErrors{Name: "Name"}
	err = engine.Flush(&entity)
	assert.NotNil(t, err)
	duplicatedError, is := err.(*orm.DuplicatedKeyError)
	assert.True(t, is)
	assert.Equal(t, "NameIndex", duplicatedError.Index)
	assert.Equal(t, "Duplicate entry 'Name' for key 'NameIndex'", duplicatedError.Error())
}
