package tests

import (
	"strconv"
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
	"github.com/summer-solutions/orm"
)

type fieldsColors struct {
	Red    string
	Green  string
	Blue   string
	Yellow string
	Purple string
}

var Color = &fieldsColors{
	Red:    "Red",
	Green:  "Green",
	Blue:   "Blue",
	Yellow: "Yellow",
	Purple: "Purple",
}

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

type TestEntityFlushTransactionLocal struct {
	orm.ORM `orm:"localCache"`
	ID      uint16
	Name    string
}

type TestEntityFlushTransactionRedis struct {
	orm.ORM `orm:"redisCache"`
	ID      uint16
	Name    string
}

func TestFlush(t *testing.T) {
	registry := &orm.Registry{}
	registry.RegisterEnum("tests.Color", Color)
	var entity TestEntityFlush
	engine := PrepareTables(t, registry, entity)

	var entities = make([]*TestEntityFlush, 10)
	for i := 1; i <= 10; i++ {
		e := TestEntityFlush{Name: "Name " + strconv.Itoa(i), EnumNotNull: Color.Red, Set: []string{Color.Red, Color.Blue}}
		engine.Track(&e)
		entities[i-1] = &e
	}
	err := engine.Flush()
	assert.Nil(t, err)
	for i := 1; i < 10; i++ {
		testEntity := entities[i-1]
		assert.Equal(t, uint16(i), testEntity.ID)
		assert.Equal(t, "Name "+strconv.Itoa(i), testEntity.Name)
		assert.False(t, engine.IsDirty(testEntity))
		assert.Nil(t, testEntity.Date)
		assert.NotNil(t, testEntity.DateNotNull)
	}

	engine.Track(entities[1], entities[7])
	entities[1].Name = "Name 2.1"
	entities[1].ReferenceOne = &TestEntityFlush{ID: 7}
	entities[7].Name = "Name 8.1"
	now := time.Now()
	entities[7].Date = &now

	assert.Nil(t, err)
	assert.True(t, engine.IsDirty(entities[1]))
	assert.True(t, engine.IsDirty(entities[7]))
	err = engine.Flush()
	assert.Nil(t, err)

	var edited1 TestEntityFlush
	var edited2 TestEntityFlush
	has, err := engine.LoadByID(2, &edited1)
	assert.True(t, has)
	assert.Nil(t, err)
	has, err = engine.LoadByID(8, &edited2)
	assert.True(t, has)
	assert.Nil(t, err)
	assert.Equal(t, "Name 2.1", edited1.Name)
	assert.Equal(t, uint16(7), edited1.ReferenceOne.ID)
	assert.Equal(t, "Name 8.1", edited2.Name)
	assert.Nil(t, edited2.ReferenceOne)
	assert.NotNil(t, edited2.Date)
	assert.Equal(t, now.Format("2006-01-02"), edited2.Date.Format("2006-01-02"))
	assert.False(t, edited1.ReferenceOne.Loaded())
	err = engine.Load(edited1.ReferenceOne)
	assert.Nil(t, err)
	assert.True(t, edited1.ReferenceOne.Loaded())
	assert.Equal(t, "Name 7", edited1.ReferenceOne.Name)

	toDelete := edited2
	engine.Track(&edited1)
	edited1.Name = "Name 2.2"
	engine.MarkToDelete(&toDelete)
	newEntity := &TestEntityFlush{Name: "Name 11", EnumNotNull: Color.Red}
	engine.Track(newEntity)
	assert.True(t, engine.IsDirty(edited1))
	assert.Nil(t, err)
	assert.True(t, engine.IsDirty(edited2))
	assert.Nil(t, err)
	assert.True(t, engine.IsDirty(newEntity))

	err = engine.Flush()
	assert.Nil(t, err)

	assert.False(t, engine.IsDirty(edited1))
	assert.False(t, engine.IsDirty(newEntity))

	var edited3 TestEntityFlush
	var deleted TestEntityFlush
	var new11 TestEntityFlush
	has, err = engine.LoadByID(2, &edited3)
	assert.True(t, has)
	assert.Nil(t, err)
	hasDeleted, err := engine.LoadByID(8, &deleted)
	assert.Nil(t, err)
	has, err = engine.LoadByID(11, &new11)
	assert.True(t, has)
	assert.Nil(t, err)
	assert.Equal(t, "Name 2.2", edited3.Name)
	assert.False(t, hasDeleted)
	assert.Equal(t, "Name 11", new11.Name)
}

func TestFlushInTransaction(t *testing.T) {
	registry := &orm.Registry{}
	registry.RegisterEnum("tests.Color", Color)
	var entity TestEntityFlushTransactionLocal
	var entity2 TestEntityFlushTransactionRedis
	engine := PrepareTables(t, registry, entity, entity2)
	logger := memory.New()
	engine.AddLogger(logger)
	engine.SetLogLevel(log.InfoLevel)

	for i := 1; i <= 10; i++ {
		e := TestEntityFlushTransactionLocal{Name: "Name " + strconv.Itoa(i)}
		engine.Track(&e)
	}
	for i := 1; i <= 10; i++ {
		e := TestEntityFlushTransactionRedis{Name: "Name " + strconv.Itoa(i)}
		engine.Track(&e)
	}
	err := engine.FlushInTransaction()
	assert.Nil(t, err)
	assert.Len(t, logger.Entries, 6)
	assert.Equal(t, "[ORM][MYSQL][BEGIN]", logger.Entries[0].Message)
	assert.Equal(t, "[ORM][MYSQL][EXEC]", logger.Entries[1].Message)
	assert.Equal(t, "[ORM][MYSQL][EXEC]", logger.Entries[2].Message)
	assert.Equal(t, "[ORM][MYSQL][COMMIT]", logger.Entries[3].Message)
	assert.Equal(t, "[ORM][LOCAL][MSET]", logger.Entries[4].Message)
	assert.Equal(t, "[ORM][REDIS][DEL]", logger.Entries[5].Message)
}

func TestFlushErrors(t *testing.T) {
	entity := &TestEntityErrors{Name: "Name"}
	engine := PrepareTables(t, &orm.Registry{}, entity)
	engine.Track(entity)

	entity.ReferenceOne = &TestEntityErrors{ID: 2}
	err := engine.Flush()
	assert.NotNil(t, err)
	keyError, is := err.(*orm.ForeignKeyError)
	assert.True(t, is)
	assert.Equal(t, "test:TestEntityErrors:ReferenceOne", keyError.Constraint)
	assert.Equal(t, "Cannot add or update a child row: a foreign key constraint fails (`test`.`TestEntityErrors`, CONSTRAINT `test:TestEntityErrors:ReferenceOne` FOREIGN KEY (`ReferenceOne`) REFERENCES `TestEntityErrors` (`ID`))", keyError.Error())
	engine.ClearTrackedEntities()

	engine.Track(entity)
	entity.ReferenceOne = nil
	err = engine.Flush()
	assert.Nil(t, err)

	entity = &TestEntityErrors{Name: "Name"}
	engine.Track(entity)
	err = engine.Flush()
	assert.NotNil(t, err)
	duplicatedError, is := err.(*orm.DuplicatedKeyError)
	assert.True(t, is)
	assert.Equal(t, "NameIndex", duplicatedError.Index)
	assert.Equal(t, "Duplicate entry 'Name' for key 'NameIndex'", duplicatedError.Error())

	engine.ClearTrackedEntities()
	err = engine.TrackAndFlush(&TestEntityErrors{ID: 1})
	assert.EqualError(t, err, "unloaded entity tests.TestEntityErrors with ID 1")
}
