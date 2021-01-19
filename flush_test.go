package orm

import (
	"context"
	"fmt"
	"testing"
	"time"

	apexLog "github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

type obj struct {
	ID         uint64
	StorageKey string
	Data       interface{}
}

type flushStruct struct {
	Name string
	Age  int
}

type flushEntity struct {
	ORM                  `orm:"localCache;redisCache"`
	ID                   uint
	City                 string `orm:"unique=city"`
	Name                 string `orm:"unique=name;required"`
	NameTranslated       map[string]string
	Age                  int
	Uint                 uint
	UintNullable         *uint
	IntNullable          *int
	Year                 uint16  `orm:"year"`
	YearNullable         *uint16 `orm:"year"`
	BoolNullable         *bool
	FloatNullable        *float64              `orm:"precision=10"`
	Float32Nullable      *float32              `orm:"precision=10"`
	ReferenceOne         *flushEntityReference `orm:"unique=ReferenceOne"`
	ReferenceTwo         *flushEntityReference `orm:"unique=ReferenceTwo"`
	ReferenceMany        []*flushEntityReference
	StringSlice          []string
	StringSliceNotNull   []string `orm:"required"`
	SetNullable          []string `orm:"set=orm.TestEnum"`
	SetNotNull           []string `orm:"set=orm.TestEnum;required"`
	EnumNullable         string   `orm:"enum=orm.TestEnum"`
	EnumNotNull          string   `orm:"enum=orm.TestEnum;required"`
	Ignored              []string `orm:"ignore"`
	Blob                 []uint8
	Bool                 bool
	FakeDelete           bool
	Float64              float64  `orm:"precision=10"`
	Decimal              float64  `orm:"decimal=5,2"`
	DecimalNullable      *float64 `orm:"decimal=5,2"`
	CachedQuery          *CachedQuery
	Time                 time.Time
	TimeWithTime         time.Time `orm:"time"`
	TimeNullable         *time.Time
	TimeWithTimeNullable *time.Time `orm:"time"`
	Interface            interface{}
	FlushStruct          flushStruct
	Int8Nullable         *int8
	Int16Nullable        *int16
	Int32Nullable        *int32
	Int64Nullable        *int64
	Uint8Nullable        *uint8
	Uint16Nullable       *uint16
	Uint32Nullable       *uint32
	Uint64Nullable       *uint64
	Images               []obj
}

type flushEntityReference struct {
	ORM
	ID   uint
	Name string
	Age  int
}

type flushEntityReferenceCascade struct {
	ORM
	ID           uint
	ReferenceOne *flushEntity
	ReferenceTwo *flushEntity `orm:"cascade"`
}

type flushEntitySmart struct {
	ORM  `orm:"localCache"`
	ID   uint
	Name string
	Age  int
}

func TestFlush(t *testing.T) {
	var entity *flushEntity
	var reference *flushEntityReference
	var referenceCascade *flushEntityReferenceCascade
	var entitySmart *flushEntitySmart
	registry := &Registry{}
	registry.RegisterEnumSlice("orm.TestEnum", []string{"a", "b", "c"})
	engine := PrepareTables(t, registry, 5, entity, reference, referenceCascade, entitySmart)

	now := time.Now()
	layout := "2006-01-02 15:04:05"
	entity = &flushEntity{Name: "Tom", Age: 12, Uint: 7, Year: 1982}
	entity.NameTranslated = map[string]string{"pl": "kot", "en": "cat"}
	entity.ReferenceOne = &flushEntityReference{Name: "John", Age: 30}
	entity.ReferenceMany = []*flushEntityReference{{Name: "Adam", Age: 18}}
	entity.StringSlice = []string{"a", "b"}
	entity.StringSliceNotNull = []string{"c", "d"}
	entity.SetNotNull = []string{"a", "b"}
	entity.EnumNotNull = "a"
	entity.TimeWithTime = now
	entity.TimeWithTimeNullable = &now
	entity.Images = []obj{{ID: 1, StorageKey: "aaa", Data: map[string]string{"sss": "vv", "bb": "cc"}}}
	assert.True(t, entity.IsDirty())
	assert.True(t, entity.ReferenceOne.IsDirty())
	flusher := engine.Flusher().Track(entity)
	flusher.Flush()
	flusher.Flush()
	assert.True(t, entity.Loaded())
	assert.True(t, entity.ReferenceOne.Loaded())
	assert.False(t, entity.IsDirty())

	assert.False(t, entity.ReferenceOne.IsDirty())
	assert.Equal(t, uint(1), entity.ID)
	assert.NotEqual(t, uint(0), entity.ReferenceOne.ID)
	assert.True(t, entity.Loaded())
	assert.True(t, entity.ReferenceOne.Loaded())
	assert.NotEqual(t, uint(0), entity.ReferenceMany[0].ID)
	assert.True(t, entity.ReferenceMany[0].Loaded())
	refOneID := entity.ReferenceOne.ID
	refManyID := entity.ReferenceMany[0].ID

	entity = &flushEntity{}
	found := engine.LoadByID(1, entity)
	assert.True(t, found)
	assert.Equal(t, "Tom", entity.Name)
	assert.Equal(t, 12, entity.Age)
	assert.Equal(t, uint(7), entity.Uint)
	assert.Equal(t, uint16(1982), entity.Year)
	assert.Equal(t, map[string]string{"pl": "kot", "en": "cat"}, entity.NameTranslated)
	assert.Equal(t, []string{"a", "b"}, entity.StringSlice)
	assert.Equal(t, []string{"c", "d"}, entity.StringSliceNotNull)
	assert.Equal(t, "", entity.EnumNullable)
	assert.Equal(t, "a", entity.EnumNotNull)
	assert.Equal(t, now.Format(layout), entity.TimeWithTime.Format(layout))
	assert.Equal(t, now.Unix(), entity.TimeWithTime.Unix())
	assert.Equal(t, now.Format(layout), entity.TimeWithTimeNullable.Format(layout))
	assert.Equal(t, now.Unix(), entity.TimeWithTimeNullable.Unix())
	assert.Nil(t, entity.SetNullable)
	assert.Equal(t, "", entity.City)
	assert.Nil(t, entity.UintNullable)
	assert.Nil(t, entity.IntNullable)
	assert.Nil(t, entity.YearNullable)
	assert.Nil(t, entity.BoolNullable)
	assert.Nil(t, entity.FloatNullable)
	assert.Nil(t, entity.Float32Nullable)
	assert.False(t, entity.IsDirty())
	assert.True(t, entity.Loaded())
	assert.False(t, entity.ReferenceOne.Loaded())
	assert.Equal(t, refOneID, entity.ReferenceOne.ID)
	assert.Nil(t, entity.Blob)
	assert.Nil(t, entity.Int8Nullable)
	assert.Nil(t, entity.Int16Nullable)
	assert.Nil(t, entity.Int32Nullable)
	assert.Nil(t, entity.Int64Nullable)
	assert.Nil(t, entity.Uint8Nullable)
	assert.Nil(t, entity.Uint16Nullable)
	assert.Nil(t, entity.Uint32Nullable)
	assert.Nil(t, entity.Uint64Nullable)

	assert.NotNil(t, entity.ReferenceMany)
	assert.Len(t, entity.ReferenceMany, 1)
	assert.Equal(t, refManyID, entity.ReferenceMany[0].ID)

	entity.ReferenceOne.Name = "John 2"
	assert.PanicsWithError(t, fmt.Sprintf("entity is not loaded and can't be updated: orm.flushEntityReference [%d]", refOneID), func() {
		engine.Flush(entity.ReferenceOne)
	})

	i := 42
	i2 := uint(42)
	i3 := uint16(1982)
	i4 := false
	i5 := 134.345
	i6 := true
	i7 := int8(4)
	i8 := int16(4)
	i9 := int32(4)
	i10 := int64(4)
	i11 := uint8(4)
	i12 := uint16(4)
	i13 := uint32(4)
	i14 := uint64(4)
	i15 := float32(134.345)
	entity.IntNullable = &i
	entity.UintNullable = &i2
	entity.Int8Nullable = &i7
	entity.Int16Nullable = &i8
	entity.Int32Nullable = &i9
	entity.Int64Nullable = &i10
	entity.Uint8Nullable = &i11
	entity.Uint16Nullable = &i12
	entity.Uint32Nullable = &i13
	entity.Uint64Nullable = &i14
	entity.YearNullable = &i3
	entity.BoolNullable = &i4
	entity.FloatNullable = &i5
	entity.Float32Nullable = &i15
	entity.City = "New York"
	entity.Blob = []uint8("Tom has a house")
	entity.Bool = true
	entity.BoolNullable = &i6
	entity.Float64 = 134.345
	entity.Decimal = 134.345
	entity.DecimalNullable = &entity.Decimal
	entity.Interface = map[string]int{"test": 12}
	entity.ReferenceMany = nil
	engine.Flush(entity)

	reference = &flushEntityReference{}
	found = engine.LoadByID(uint64(refOneID), reference)
	assert.True(t, found)
	assert.Equal(t, "John", reference.Name)
	assert.Equal(t, 30, reference.Age)

	entity = &flushEntity{}
	engine.LoadByID(1, entity)
	assert.Equal(t, 42, *entity.IntNullable)
	assert.Equal(t, int8(4), *entity.Int8Nullable)
	assert.Equal(t, int16(4), *entity.Int16Nullable)
	assert.Equal(t, int32(4), *entity.Int32Nullable)
	assert.Equal(t, int64(4), *entity.Int64Nullable)
	assert.Equal(t, uint8(4), *entity.Uint8Nullable)
	assert.Equal(t, uint16(4), *entity.Uint16Nullable)
	assert.Equal(t, uint32(4), *entity.Uint32Nullable)
	assert.Equal(t, uint64(4), *entity.Uint64Nullable)
	assert.Equal(t, uint(42), *entity.UintNullable)
	assert.Equal(t, uint16(1982), *entity.YearNullable)
	assert.True(t, *entity.BoolNullable)
	assert.True(t, entity.Bool)
	assert.Equal(t, 134.345, *entity.FloatNullable)
	assert.Equal(t, float32(134.345), *entity.Float32Nullable)
	assert.Equal(t, "New York", entity.City)
	assert.Equal(t, []uint8("Tom has a house"), entity.Blob)
	assert.Equal(t, 134.345, entity.Float64)
	assert.Equal(t, 134.35, entity.Decimal)
	assert.Equal(t, 134.35, *entity.DecimalNullable)
	assert.Nil(t, entity.ReferenceMany)
	assert.False(t, entity.IsDirty())
	assert.False(t, reference.IsDirty())
	assert.True(t, reference.Loaded())

	entity.ReferenceMany = []*flushEntityReference{}
	assert.False(t, entity.IsDirty())
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(1, entity)
	assert.Nil(t, entity.ReferenceMany)

	entity2 := &flushEntity{Name: "Tom", Age: 12, EnumNotNull: "a"}
	assert.PanicsWithError(t, "Duplicate entry 'Tom' for key 'name'", func() {
		engine.Flush(entity2)
	})

	entity2.Name = "Lucas"
	entity2.ReferenceOne = &flushEntityReference{ID: 3}
	assert.PanicsWithError(t, "foreign key error in key `test:flushEntity:ReferenceOne`", func() {
		engine.Flush(entity2)
	}, "")

	entity2.ReferenceOne = nil
	entity2.Name = "Tom"
	entity2.SetOnDuplicateKeyUpdate(Bind{"Age": 40})
	engine.Flush(entity2)

	assert.Equal(t, uint(1), entity2.ID)
	assert.Equal(t, 40, entity2.Age)
	engine.LoadByID(1, entity)
	assert.Equal(t, "Tom", entity.Name)
	assert.Equal(t, 40, entity.Age)

	entity2 = &flushEntity{Name: "Tom", Age: 12, EnumNotNull: "a"}
	entity2.SetOnDuplicateKeyUpdate(Bind{})
	engine.Flush(entity2)
	assert.Equal(t, uint(1), entity2.ID)

	entity2 = &flushEntity{Name: "Arthur", Age: 18, EnumNotNull: "a"}
	entity2.ReferenceTwo = reference
	entity2.SetOnDuplicateKeyUpdate(Bind{})
	engine.Flush(entity2)
	assert.Equal(t, uint(6), entity2.ID)

	entity2 = &flushEntity{Name: "Adam", Age: 20, ID: 10, EnumNotNull: "a"}
	engine.Flush(entity2)
	found = engine.LoadByID(10, entity2)
	assert.True(t, found)

	entity2.Age = 21
	entity2.UintNullable = &i2
	entity2.BoolNullable = &i4
	entity2.FloatNullable = &i5
	entity2.City = "Warsaw"
	assert.True(t, entity2.IsDirty())
	engine.Flush(entity2)
	assert.False(t, entity2.IsDirty())
	engine.LoadByID(10, entity2)
	assert.Equal(t, 21, entity2.Age)

	entity2.UintNullable = nil
	entity2.BoolNullable = nil
	entity2.FloatNullable = nil
	entity2.City = ""
	assert.True(t, entity2.IsDirty())
	engine.Flush(entity2)
	assert.False(t, entity2.IsDirty())
	entity2 = &flushEntity{}
	engine.LoadByID(10, entity2)
	assert.Nil(t, entity2.UintNullable)
	assert.Nil(t, entity2.BoolNullable)
	assert.Nil(t, entity2.FloatNullable)
	assert.Equal(t, "", entity2.City)

	entity2.MarkToDelete()
	assert.True(t, entity2.IsDirty())
	engine.Flush(entity2)
	found = engine.LoadByID(10, entity2)
	assert.True(t, found)
	assert.True(t, entity2.FakeDelete)

	referenceCascade = &flushEntityReferenceCascade{ReferenceOne: entity}
	engine.Flush(referenceCascade)
	entity.ForceMarkToDelete()
	assert.True(t, entity.IsDirty())
	assert.PanicsWithError(t, "foreign key error in key `test:flushEntityReferenceCascade:ReferenceOne`", func() {
		engine.Flush(entity)
	})
	referenceCascade.ReferenceOne = nil
	referenceCascade.ReferenceTwo = entity
	engine.Flush(referenceCascade)
	engine.LoadByID(1, referenceCascade)
	assert.Nil(t, referenceCascade.ReferenceOne)
	assert.NotNil(t, referenceCascade.ReferenceTwo)
	assert.Equal(t, uint(1), referenceCascade.ReferenceTwo.ID)
	entity.MarkToDelete()
	engine.Flush(entity)
	found = engine.LoadByID(1, referenceCascade)
	assert.False(t, found)

	engine.Flush(&flushEntity{Name: "Tom", Age: 12, Uint: 7, Year: 1982, EnumNotNull: "a"})
	entity3 := &flushEntity{}
	found = engine.LoadByID(11, entity3)
	assert.True(t, found)
	assert.Nil(t, entity3.NameTranslated)

	engine.Flush(&flushEntity{SetNullable: []string{}, EnumNotNull: "a"})
	entity4 := &flushEntity{}
	found = engine.LoadByID(12, entity4)
	assert.True(t, found)
	assert.Nil(t, entity4.SetNullable)
	assert.Equal(t, []string{}, entity4.SetNotNull)
	entity4.SetNullable = []string{"a", "c"}
	engine.Flush(entity4)
	entity4 = &flushEntity{}
	engine.LoadByID(12, entity4)
	assert.Equal(t, []string{"a", "c"}, entity4.SetNullable)

	engine.GetMysql().Begin()
	entity4.ForceMarkToDelete()
	entity5 := &flushEntity{Name: "test_transaction", EnumNotNull: "a"}
	engine.Flush(entity5)
	entity5.Age = 38
	engine.Flush(entity5)
	engine.GetMysql().Commit()
	entity5 = &flushEntity{}
	found = engine.LoadByID(13, entity5)
	assert.True(t, found)
	assert.Equal(t, "test_transaction", entity5.Name)
	assert.Equal(t, 38, entity5.Age)

	entity6 := &flushEntity{Name: "test_transaction_2", EnumNotNull: "a"}
	flusher.Clear()
	flusher.FlushInTransaction()
	flusher.Track(entity6)
	flusher.FlushInTransaction()
	entity6 = &flushEntity{}
	found = engine.LoadByID(14, entity6)
	assert.True(t, found)
	assert.Equal(t, "test_transaction_2", entity6.Name)

	entity7 := &flushEntity{Name: "test_lock", EnumNotNull: "a"}
	flusher.Track(entity7)
	flusher.FlushWithLock("default", "lock_test", time.Second, time.Second)
	entity7 = &flushEntity{}
	found = engine.LoadByID(15, entity7)
	assert.True(t, found)
	assert.Equal(t, "test_lock", entity7.Name)

	lock, has := engine.GetLocker().Obtain(engine.context, "lock_test", time.Second, time.Second)
	assert.True(t, has)
	assert.PanicsWithError(t, "lock wait timeout", func() {
		flusher.FlushWithLock("default", "lock_test", time.Second, time.Second)
	})
	lock.Release()

	entity8 := &flushEntity{Name: "test_check", EnumNotNull: "a"}
	flusher.Track(entity8)
	err := flusher.FlushWithCheck()
	assert.NoError(t, err)
	entity8 = &flushEntity{}
	found = engine.LoadByID(16, entity8)
	assert.True(t, found)
	assert.Equal(t, "test_check", entity8.Name)

	entity8 = &flushEntity{Name: "test_check", EnumNotNull: "a"}
	flusher.Track(entity8)
	err = flusher.FlushWithCheck()
	assert.EqualError(t, err, "Duplicate entry 'test_check' for key 'name'")
	entity8 = &flushEntity{Name: "test_check_2", EnumNotNull: "a", ReferenceOne: &flushEntityReference{ID: 100}}
	flusher.Track(entity8)
	err = flusher.FlushWithCheck()
	assert.EqualError(t, err, "foreign key error in key `test:flushEntity:ReferenceOne`")

	entity8 = &flushEntity{Name: "test_check_3", EnumNotNull: "Y"}
	flusher.Track(entity8)
	err = flusher.FlushWithFullCheck()
	assert.EqualError(t, err, "Error 1265: Data truncated for column 'EnumNotNull' at row 1")
	flusher.Track(entity8)
	assert.Panics(t, func() {
		_ = flusher.FlushWithCheck()
	})

	entity9 := &flushEntity{Name: "test_check", EnumNotNull: "a"}
	flusher.Track(entity9)
	err = flusher.FlushInTransactionWithCheck()
	assert.EqualError(t, err, "Duplicate entry 'test_check' for key 'name'")

	entity9 = &flushEntity{Name: "test_check_5", EnumNotNull: "a"}
	flusher.Track(entity9)
	flusher.FlushInTransactionWithLock("default", "lock_test", time.Second, time.Second)
	entity9 = &flushEntity{}
	found = engine.LoadByID(20, entity9)
	assert.True(t, found)
	assert.Equal(t, "test_check_5", entity9.Name)

	assert.PanicsWithError(t, "track limit 10000 exceeded", func() {
		for i := 1; i <= 10001; i++ {
			flusher.Track(&flushEntity{})
		}
	})

	flusher.Clear()
	entity2 = &flushEntity{ID: 100, Age: 1, EnumNotNull: "a"}
	entity2.SetOnDuplicateKeyUpdate(Bind{"Age": 2})
	engine.Flush(entity2)
	assert.Equal(t, uint(12), entity2.ID)
	assert.Equal(t, 2, entity2.Age)
	entity2 = &flushEntity{}
	found = engine.LoadByID(100, entity2)
	assert.False(t, found)
	entity2 = &flushEntity{Name: "Frank", ID: 100, Age: 1, EnumNotNull: "a"}
	entity2.SetOnDuplicateKeyUpdate(Bind{"Age": 2})
	engine.Flush(entity2)
	found = engine.LoadByID(100, entity2)
	assert.True(t, found)
	assert.Equal(t, 1, entity2.Age)

	entity2 = &flushEntity{ID: 100, Age: 1, EnumNotNull: "a"}
	entity2.SetOnDuplicateKeyUpdate(Bind{"Age": 2})
	engine.Flush(entity2)
	assert.Equal(t, uint(100), entity2.ID)
	assert.Equal(t, 2, entity2.Age)
	entity2 = &flushEntity{}
	found = engine.LoadByID(100, entity2)
	assert.True(t, found)
	assert.Equal(t, 2, entity2.Age)

	entitySmart = &flushEntitySmart{Name: "Test", Age: 18}
	engine.Flush(entitySmart)
	assert.Equal(t, uint(1), entitySmart.ID)
	entitySmart = &flushEntitySmart{}
	found = engine.LoadByID(1, entitySmart)
	assert.True(t, found)
	entitySmart.Age = 20

	receiver := NewAsyncConsumer(engine, "default-consumer", 1)
	receiver.DisableLoop()
	receiver.block = time.Millisecond

	testLogger := memory.New()
	engine.AddQueryLogger(testLogger, apexLog.InfoLevel, QueryLoggerSourceDB)
	engine.Flush(entitySmart)
	entitySmart = &flushEntitySmart{}
	found = engine.LoadByID(1, entitySmart)
	assert.True(t, found)
	assert.Equal(t, 20, entitySmart.Age)
	assert.Len(t, testLogger.Entries, 0)
	validHeartBeat := false
	receiver.SetHeartBeat(time.Minute, func() {
		validHeartBeat = true
	})
	receiver.Digest(context.Background(), 100)
	assert.True(t, validHeartBeat)
	assert.Len(t, testLogger.Entries, 1)
	assert.Equal(t, "UPDATE flushEntitySmart SET `Age` = ? WHERE `ID` = ?", testLogger.Entries[0].Fields["Query"])

	entity = &flushEntity{Name: "Monica", EnumNotNull: "a", ReferenceMany: []*flushEntityReference{{Name: "Adam Junior"}}}
	engine.Flush(entity)
	assert.Equal(t, uint(101), entity.ID)
	assert.Equal(t, uint(3), entity.ReferenceMany[0].ID)

	entity = &flushEntity{Name: "John", EnumNotNull: "a", ReferenceMany: []*flushEntityReference{}}
	engine.Flush(entity)
	entity = &flushEntity{}
	engine.LoadByID(102, entity)
	assert.Nil(t, entity.ReferenceMany)

	flusher = engine.Flusher()
	entity = &flushEntity{Name: "Irena", EnumNotNull: "a"}
	flusher.Track(entity)
	ref1 := &flushEntityReferenceCascade{ReferenceOne: entity}
	ref2 := &flushEntityReferenceCascade{ReferenceOne: entity}
	flusher.Track(ref1, ref2)

	flusher.Flush()
	assert.Equal(t, uint(103), entity.ID)
	assert.Equal(t, uint(103), ref1.ReferenceOne.ID)
	assert.Equal(t, uint(103), ref2.ReferenceOne.ID)
	assert.Equal(t, uint(2), ref1.ID)
	assert.Equal(t, uint(3), ref2.ID)
}
