package orm

import (
	"strconv"
	"testing"
	"time"

	"github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

type testEntityFlush struct {
	ORM
	ID           uint16
	Name         string
	Number       int16
	NameNotNull  string `orm:"required"`
	Blob         []byte
	Enum         string   `orm:"enum=orm.colorEnum"`
	EnumNotNull  string   `orm:"enum=orm.colorEnum;required"`
	Year         uint16   `orm:"year=true"`
	YearNotNull  uint16   `orm:"year=true;required"`
	Set          []string `orm:"set=orm.colorEnum;required"`
	Date         *time.Time
	DateNotNull  time.Time  `orm:"required"`
	DateTime     *time.Time `orm:"time"`
	ReferenceOne *testEntityFlush
	Ignored      []time.Time `orm:"ignore"`
}

type testEntityErrors struct {
	ORM
	ID           uint16
	Name         string `orm:"unique=NameIndex"`
	ReferenceOne *testEntityErrors
}

type testEntityFlushTransactionLocal struct {
	ORM  `orm:"localCache"`
	ID   uint16
	Name string
}

type testEntityFlushTransactionRedis struct {
	ORM  `orm:"redisCache"`
	ID   uint16
	Name string
}

func TestFlush(t *testing.T) {
	registry := &Registry{}
	registry.RegisterEnumStruct("orm.colorEnum", colorEnum)
	registry.RegisterLocker("default", "default")
	var entity testEntityFlush
	engine := PrepareTables(t, registry, entity)

	var entities = make([]*testEntityFlush, 10)
	for i := 1; i <= 10; i++ {
		e := &testEntityFlush{Name: "Name " + strconv.Itoa(i), EnumNotNull: "Red", Set: []string{"Red", "Blue"}}
		engine.Track(e)
		entities[i-1] = e
	}
	engine.Flush()
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
	entities[1].ReferenceOne = &testEntityFlush{ID: 7}
	entities[7].Name = "Name 8.1"
	now := time.Now()
	entities[7].Date = &now
	entities[7].DateTime = &now

	assert.True(t, engine.IsDirty(entities[1]))
	assert.True(t, engine.IsDirty(entities[7]))
	engine.Flush()

	var edited1 testEntityFlush
	var edited2 testEntityFlush
	has := engine.LoadByID(2, &edited1)
	assert.True(t, has)
	has = engine.LoadByID(8, &edited2)
	assert.True(t, has)
	assert.Equal(t, "Name 2.1", edited1.Name)
	assert.Equal(t, uint16(7), edited1.ReferenceOne.ID)
	assert.Equal(t, "Name 8.1", edited2.Name)
	assert.Nil(t, edited2.ReferenceOne)
	assert.NotNil(t, edited2.Date)
	assert.Equal(t, now.Format("2006-01-02"), edited2.Date.Format("2006-01-02"))
	assert.NotNil(t, edited2.DateTime)
	assert.Equal(t, now.Format("2006-01-02 15:04:05"), edited2.DateTime.Format("2006-01-02 15:04:05"))
	assert.False(t, engine.Loaded(edited1.ReferenceOne))
	engine.Load(edited1.ReferenceOne)
	assert.True(t, engine.Loaded(edited1.ReferenceOne))
	assert.Equal(t, "Name 7", edited1.ReferenceOne.Name)

	toDelete := edited2
	engine.Track(&edited1)
	edited1.Name = "Name 2.2"
	engine.MarkToDelete(&toDelete)
	newEntity := &testEntityFlush{Name: "Name 11", EnumNotNull: "Red"}
	engine.Track(newEntity)
	assert.True(t, engine.IsDirty(&edited1))
	assert.True(t, engine.IsDirty(&edited2))
	assert.True(t, engine.IsDirty(newEntity))

	engine.Flush()

	assert.False(t, engine.IsDirty(&edited1))
	assert.False(t, engine.IsDirty(newEntity))

	var edited3 testEntityFlush
	var deleted testEntityFlush
	var new11 testEntityFlush
	has = engine.LoadByID(2, &edited3)
	assert.True(t, has)
	hasDeleted := engine.LoadByID(8, &deleted)
	has = engine.LoadByID(11, &new11)
	assert.True(t, has)
	assert.Equal(t, "Name 2.2", edited3.Name)
	assert.False(t, hasDeleted)
	assert.Equal(t, "Name 11", new11.Name)

	logger := memory.New()
	engine.AddQueryLogger(logger, log.InfoLevel)
	for i := 100; i <= 110; i++ {
		e := testEntityFlush{Name: "Name " + strconv.Itoa(i), EnumNotNull: "Red"}
		assert.Equal(t, uint64(0), e.GetID())
		engine.Track(&e)
	}
	logger.Entries = make([]*log.Entry, 0)
	engine.FlushWithLock("default", "test", time.Second, time.Second)
	assert.Len(t, logger.Entries, 3)
	assert.Equal(t, "[ORM][LOCKER][OBTAIN]", logger.Entries[0].Message)
	assert.Equal(t, "[ORM][MYSQL][EXEC]", logger.Entries[1].Message)
	assert.Equal(t, "[ORM][LOCKER][RELEASE]", logger.Entries[2].Message)
}

func TestFlushInTransaction(t *testing.T) {
	registry := &Registry{}
	registry.RegisterEnumStruct("orm.colorEnum", colorEnum)
	registry.RegisterLocker("default", "default")
	var entity testEntityFlushTransactionLocal
	var entity2 testEntityFlushTransactionRedis
	engine := PrepareTables(t, registry, entity, entity2)
	logger := memory.New()
	engine.AddQueryLogger(logger, log.InfoLevel, LoggerSourceRedis, LoggerSourceDB, LoggerSourceLocalCache)

	for i := 1; i <= 10; i++ {
		e := testEntityFlushTransactionLocal{Name: "Name " + strconv.Itoa(i)}
		engine.Track(&e)
	}
	for i := 1; i <= 10; i++ {
		e := testEntityFlushTransactionRedis{Name: "Name " + strconv.Itoa(i)}
		engine.Track(&e)
	}
	engine.FlushInTransaction()
	assert.Len(t, logger.Entries, 6)
	assert.Equal(t, "[ORM][MYSQL][BEGIN]", logger.Entries[0].Message)
	assert.Equal(t, "[ORM][MYSQL][EXEC]", logger.Entries[1].Message)
	assert.Equal(t, "[ORM][MYSQL][EXEC]", logger.Entries[2].Message)
	assert.Equal(t, "[ORM][MYSQL][COMMIT]", logger.Entries[3].Message)
	assert.Equal(t, "[ORM][LOCAL][MSET]", logger.Entries[4].Message)
	assert.Equal(t, "[ORM][REDIS][DEL]", logger.Entries[5].Message)

	for i := 100; i <= 110; i++ {
		e := testEntityFlushTransactionLocal{Name: "Name " + strconv.Itoa(i)}
		engine.Track(&e)
	}
	logger.Entries = make([]*log.Entry, 0)
	engine.FlushInTransactionWithLock("default", "test", time.Second, time.Second)
	assert.Len(t, logger.Entries, 6)
	assert.Equal(t, "[ORM][LOCKER][OBTAIN]", logger.Entries[0].Message)
	assert.Equal(t, "[ORM][LOCKER][RELEASE]", logger.Entries[5].Message)
}

func TestFlushErrors(t *testing.T) {
	entity := &testEntityErrors{Name: "Name"}
	engine := PrepareTables(t, &Registry{}, entity)
	engine.Track(entity)

	entity.ReferenceOne = &testEntityErrors{ID: 2}
	err := engine.FlushWithCheck()
	assert.NotNil(t, err)
	assert.Equal(t, "test:testEntityErrors:ReferenceOne", err.(*ForeignKeyError).Constraint)
	assert.Equal(t, "Cannot add or update a child row: a foreign key constraint fails (`test`.`testEntityErrors`, CONSTRAINT `test:testEntityErrors:ReferenceOne` FOREIGN KEY (`ReferenceOne`) REFERENCES `testEntityErrors` (`ID`))", err.Error())
	engine.ClearTrackedEntities()

	engine.Track(entity)
	entity.ReferenceOne = nil
	engine.Flush()

	entity = &testEntityErrors{Name: "Name"}
	engine.Track(entity)
	duplicatedError := engine.FlushWithCheck()
	assert.NotNil(t, duplicatedError)
	assert.Equal(t, "NameIndex", duplicatedError.(*DuplicatedKeyError).Index)
	assert.Equal(t, "Duplicate entry 'Name' for key 'NameIndex'", duplicatedError.Error())
}
