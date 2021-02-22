package orm

import (
	"strconv"
	"strings"
	"testing"
	"time"

	apexLog "github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

func TestRedisSearch(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6383", 0)
	testIndex := &RedisSearchIndex{Name: "test", RedisPool: "default", PayloadField: "_my_payload",
		ScoreField: "_my_score", LanguageField: "_my_language", DefaultScore: 0.8,
		Prefixes: []string{"doc1:", "doc2:"}, StopWords: []string{"and", "in"}}
	testIndex.MaxTextFields = true
	testIndex.NoOffsets = true
	testIndex.NoNHL = true // TODO why not visible in info
	testIndex.NoFields = true
	testIndex.NoFreqs = true
	testIndex.AddTextField("title", 0.4, true, false, false)
	testIndex.AddTextField("test", 1, false, true, true)
	testIndex.AddNumericField("age", true, false)
	testIndex.AddGeoField("location", false, false)
	testIndex.AddTagField("tags", true, false, ".")
	registry.RegisterRedisSearchIndex(testIndex)
	testIndex2 := &RedisSearchIndex{Name: "test2", RedisPool: "default", Prefixes: []string{"test2:"}}
	testIndex2.AddTextField("title", 1, true, false, false)
	registry.RegisterRedisSearchIndex(testIndex2)
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	engine := validatedRegistry.CreateEngine()
	engine.GetRedis().FlushDB()

	testLog := memory.New()
	engine.AddQueryLogger(testLog, apexLog.InfoLevel, QueryLoggerSourceRedis)

	search := engine.GetRedisSearch()
	assert.NotNil(t, search)
	alters := engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters, 2)

	search.createIndex(&RedisSearchIndex{Name: "to_delete", RedisPool: "default"})

	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters, 3)
	assert.Equal(t, "default", alters[0].Pool)
	assert.Equal(t, "FT.DROPINDEX to_delete DD", alters[0].Query)
	assert.True(t, alters[0].Safe)
	alters[0].Execute()
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters, 2)
	for _, alter := range alters {
		if strings.Contains(alter.Query, "test2") {
			assert.Equal(t, "FT.CREATE test2 ON HASH PREFIX 1 test2: SCHEMA title TEXT SORTABLE", alter.Query)
		} else {
			assert.Equal(t, "FT.CREATE test ON HASH PREFIX 2 doc1: doc2: LANGUAGE_FIELD _my_language SCORE 0.8 SCORE_FIELD _my_score PAYLOAD_FIELD _my_payload MAXTEXTFIELDS NOOFFSETS NOHL NOFIELDS NOFREQS STOPWORDS 2 and in SCHEMA title TEXT WEIGHT 0.4 SORTABLE test TEXT NOSTEM NOINDEX age NUMERIC SORTABLE location GEO tags TAG SEPARATOR . SORTABLE", alter.Query)
		}
		assert.Equal(t, "default", alter.Pool)
		assert.True(t, alter.Safe)
	}

	alters[0].Execute()
	alters[1].Execute()

	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters, 0)

	info := search.info("test")
	assert.Equal(t, "test", info.Name)
	assert.Equal(t, "_my_payload", info.Definition.PayloadField)
	assert.Equal(t, "_my_score", info.Definition.ScoreField)
	assert.Equal(t, "_my_language", info.Definition.LanguageField)
	assert.Equal(t, 0.8, info.Definition.DefaultScore)
	assert.Len(t, info.Definition.Prefixes, 2)
	assert.Equal(t, "doc1:", info.Definition.Prefixes[0])
	assert.Equal(t, "doc2:", info.Definition.Prefixes[1])
	assert.Equal(t, []string{"and", "in"}, info.StopWords)
	hasMaxTextFields := false
	hasNoOffsets := false
	hasNoFields := false
	hasNoFreqs := false
	for _, val := range info.Options {
		switch val {
		case "MAXTEXTFIELDS":
			hasMaxTextFields = true
		case "NOOFFSETS":
			hasNoOffsets = true
		case "NOFIELDS":
			hasNoFields = true
		case "NOFREQS":
			hasNoFreqs = true
		}
	}
	assert.True(t, hasMaxTextFields)
	assert.True(t, hasNoOffsets)
	assert.True(t, hasNoFields)
	assert.True(t, hasNoFreqs)
	assert.Len(t, info.Fields, 5)
	assert.Equal(t, "title", info.Fields[0].Name)
	assert.Equal(t, "TEXT", info.Fields[0].Type)
	assert.Equal(t, 0.4, info.Fields[0].Weight)
	assert.True(t, info.Fields[0].Sortable)
	assert.False(t, info.Fields[0].NoIndex)
	assert.False(t, info.Fields[0].NoSteam)
	assert.Equal(t, "test", info.Fields[1].Name)
	assert.Equal(t, "TEXT", info.Fields[1].Type)
	assert.Equal(t, 1.0, info.Fields[1].Weight)
	assert.False(t, info.Fields[1].Sortable)
	assert.True(t, info.Fields[1].NoIndex)
	assert.True(t, info.Fields[1].NoSteam)
	assert.Equal(t, "age", info.Fields[2].Name)
	assert.Equal(t, "NUMERIC", info.Fields[2].Type)
	assert.True(t, info.Fields[2].Sortable)
	assert.Equal(t, "location", info.Fields[3].Name)
	assert.Equal(t, "GEO", info.Fields[3].Type)
	assert.False(t, info.Fields[3].Sortable)
	assert.Equal(t, "tags", info.Fields[4].Name)
	assert.Equal(t, "TAG", info.Fields[4].Type)
	assert.True(t, info.Fields[4].Sortable)
	assert.Equal(t, ".", info.Fields[4].TagSeparator)

	pipeline := engine.GetRedis().PipeLine()
	for i := 1; i <= 1000; i++ {
		pipeline.HSet("test2:"+strconv.Itoa(i), "title", "hello "+strconv.Itoa(i), "id", i)
	}
	pipeline.Exec()

	search.aliasUpdate("test2_alias", "test2")

	testIndex3 := &RedisSearchIndex{Name: "test3", RedisPool: "default", Prefixes: []string{"test2:"}}
	testIndex3.AddTextField("title", 1, true, false, false)
	testIndex3.AddTextField("title2", 1, false, false, false)
	testIndex3.AddNumericField("id", true, false)
	testIndex3.AddNumericField("number_signed", false, false)
	testIndex3.AddNumericField("number_float", true, false)
	testIndex3.AddNumericField("sort_test", true, false)
	testIndex3.AddGeoField("location", true, false)
	testIndex3.LanguageField = "lang"
	search.createIndex(testIndex3)
	time.Sleep(time.Millisecond * 100)
	engine.GetRedis().HSet("test2:33", "number_signed", -10)
	engine.GetRedis().HSet("test2:33", "number_float", 2.5)
	engine.GetRedis().HSet("test2:33", "location", "52.2982648,17.0103596")
	engine.GetRedis().HSet("test2:33", "sort_test", 30)
	engine.GetRedis().HSet("test2:33", "title2", "hello 33 friend tom")
	engine.GetRedis().HSet("test2:34", "number_signed", 10)
	engine.GetRedis().HSet("test2:34", "number_float", 7.34)
	engine.GetRedis().HSet("test2:34", "location", "52.5248822,17.5681129")
	engine.GetRedis().HSet("test2:34", "sort_test", 30)
	engine.GetRedis().HSet("test2:35", "number_signed", 5)
	engine.GetRedis().HSet("test2:35", "number_float", 8.12)
	engine.GetRedis().HSet("test2:35", "location", "52.2328546,20.9207698")
	engine.GetRedis().HSet("test2:35", "sort_test", 20)

	search.aliasUpdate("test2_alias", "test3")
	query := &RedisSearchQuery{}
	query.Query("hello").Verbatim().NoStopWords()

	total, rowsRaw := search.SearchRaw("test2_alias", query, NewPager(1, 2))
	assert.Len(t, rowsRaw, 4)
	assert.Equal(t, int64(1000), total)

	total, keys := search.SearchKeys("test2_alias", query, NewPager(1, 2))
	assert.Len(t, keys, 2)
	assert.Equal(t, int64(1000), total)
	assert.Equal(t, "test2:35", keys[0])
	assert.Equal(t, "test2:34", keys[1])

	_, rows := search.Search("test2_alias", query, NewPager(1, 2))
	assert.Len(t, rows, 2)
	assert.Equal(t, "test2:35", rows[0].Key)
	assert.Equal(t, "35", rows[0].Value("id"))
	assert.Equal(t, "5", rows[0].Value("number_signed"))
	assert.Equal(t, "8.12", rows[0].Value("number_float"))
	assert.Equal(t, "52.2328546,20.9207698", rows[0].Value("location"))
	assert.Equal(t, "test2:34", rows[1].Key)
	assert.Equal(t, "hello 34", rows[1].Value("title"))
	assert.Equal(t, "34", rows[1].Value("id"))
	assert.Equal(t, "10", rows[1].Value("number_signed"))
	assert.Equal(t, "7.34", rows[1].Value("number_float"))
	assert.Equal(t, "52.5248822,17.5681129", rows[1].Value("location"))

	query.WithScores().WithPayLoads()
	_, rows = search.Search("test2_alias", query, NewPager(1, 2))
	assert.Len(t, rows, 2)
	assert.Equal(t, "test2:35", rows[0].Key)
	assert.Equal(t, "test2:34", rows[1].Key)
	assert.Equal(t, "hello 35", rows[0].Value("title"))
	assert.Equal(t, "hello 34", rows[1].Value("title"))

	//engine.EnableQueryDebug()
	query = &RedisSearchQuery{}
	query.FilterInt("id", 34, 34)
	total, rows = search.Search("test2_alias", query, NewPager(1, 2))
	assert.Equal(t, int64(1), total)
	assert.Len(t, rows, 1)
	assert.Equal(t, "test2:34", rows[0].Key)

	query = &RedisSearchQuery{}
	query.FilterInt("id", 33, 35).FilterInt("number_signed", -10, 5)
	total, rows = search.Search("test2_alias", query, NewPager(1, 2))
	assert.Equal(t, int64(2), total)
	assert.Len(t, rows, 2)
	assert.Equal(t, "test2:35", rows[0].Key)
	assert.Equal(t, "test2:33", rows[1].Key)

	query = &RedisSearchQuery{}
	query.FilterFloat("number_float", 7.33, 7.35)
	total, rows = search.Search("test2_alias", query, NewPager(1, 2))
	assert.Equal(t, int64(1), total)
	assert.Len(t, rows, 1)
	assert.Equal(t, "test2:34", rows[0].Key)

	query = &RedisSearchQuery{}
	query.FilterGeo("location", 52.2982648, 17.0103596, 75, "km")
	total, rows = search.Search("test2_alias", query, NewPager(1, 2))
	assert.Equal(t, int64(2), total)
	assert.Len(t, rows, 2)
	assert.Equal(t, "test2:34", rows[0].Key)
	assert.Equal(t, "test2:33", rows[1].Key)

	query = &RedisSearchQuery{}
	query.FilterInt("id", 1, 100).Sort("id", false)
	total, rows = search.Search("test2_alias", query, NewPager(1, 3))
	assert.Equal(t, int64(100), total)
	assert.Len(t, rows, 3)
	assert.Equal(t, "test2:1", rows[0].Key)
	assert.Equal(t, "test2:2", rows[1].Key)
	assert.Equal(t, "test2:3", rows[2].Key)

	query = &RedisSearchQuery{}
	query.FilterInt("id", 1, 100).Sort("id", true)
	total, rows = search.Search("test2_alias", query, NewPager(1, 3))
	assert.Equal(t, int64(100), total)
	assert.Len(t, rows, 3)
	assert.Equal(t, "test2:100", rows[0].Key)
	assert.Equal(t, "test2:99", rows[1].Key)
	assert.Equal(t, "test2:98", rows[2].Key)

	query.InKeys("test2:100", "test2:98")
	query.FilterInt("id", 1, 100).Sort("id", true)
	total, rows = search.Search("test2_alias", query, NewPager(1, 3))
	assert.Equal(t, int64(2), total)
	assert.Len(t, rows, 2)
	assert.Equal(t, "test2:100", rows[0].Key)
	assert.Equal(t, "test2:98", rows[1].Key)

	query = &RedisSearchQuery{}
	query.Query("hello").InFields("title2").Return("id", "title2")
	total, rows = search.Search("test2_alias", query, NewPager(1, 3))
	assert.Equal(t, int64(1), total)
	assert.Len(t, rows, 1)
	assert.Equal(t, "test2:33", rows[0].Key)
	assert.Len(t, rows[0].Fields, 4)
	assert.Equal(t, "id", rows[0].Fields[0])
	assert.Equal(t, "33", rows[0].Fields[1])
	assert.Equal(t, "title2", rows[0].Fields[2])
	assert.Equal(t, "hello 33 friend tom", rows[0].Fields[3])

	query = &RedisSearchQuery{}
	query.Query("hello tom").WithScores().ExplainScore()
	total, rows = search.Search("test2_alias", query, NewPager(1, 3))
	assert.Equal(t, int64(1), total)
	assert.Equal(t, 1.5, rows[0].Score)
	assert.NotNil(t, rows[0].ExplainScore)
	assert.Equal(t, "test2:33", rows[0].Key)
	query.Slop(0)
	total, _ = search.Search("test2_alias", query, NewPager(1, 3))
	assert.Equal(t, int64(0), total)

	query = &RedisSearchQuery{}
	query.Query("tom hello")
	total, _ = search.Search("test2_alias", query, NewPager(1, 3))
	assert.Equal(t, int64(1), total)
	query.InOrder()
	total, _ = search.Search("test2_alias", query, NewPager(1, 3))
	assert.Equal(t, int64(0), total)

	query = &RedisSearchQuery{}
	query.Query("hello").Lang("german")
	total, _ = search.Search("test2_alias", query, NewPager(1, 3))
	assert.Equal(t, int64(1000), total)

	search.dropIndex("test2")
}
