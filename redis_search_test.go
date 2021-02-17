package orm

import (
	"testing"

	apexLog "github.com/apex/log"
	"github.com/apex/log/handlers/memory"

	"github.com/stretchr/testify/assert"
)

func TestRedisSearch(t *testing.T) {
	registry := &Registry{}
	registry.RegisterRedis("localhost:6383", 0)
	validatedRegistry, err := registry.Validate()
	assert.NoError(t, err)
	engine := validatedRegistry.CreateEngine()
	engine.GetRedis().FlushDB()

	testLog := memory.New()
	engine.AddQueryLogger(testLog, apexLog.InfoLevel, QueryLoggerSourceRedis)

	search := engine.GetRedisSearch()
	assert.NotNil(t, search)
	alters := engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters, 0)

	testIndex := RedisSearchIndex{Name: "test", RedisPool: "default", PayloadField: "_my_payload",
		ScoreField: "_my_score", LanguageField: "_my_language", DefaultScore: 0.8,
		Prefixes: []string{"doc1:", "doc2:"}, StopWords: []string{"and", "in"}}
	testIndex.MaxTextFields = true
	testIndex.NoOffsets = true
	testIndex.NoNHL = true // TODO why not visible in info
	testIndex.NoFields = true
	testIndex.NoFreqs = true
	testIndex.AddTextField("title", 1, true, false, false)
	testIndex.AddNumericField("age", true, false)
	testIndex.AddGeoField("location", true, false)
	testIndex.AddTagField("tags", true, false, "")
	search.createIndex(testIndex)

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

	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters, 1)
	assert.Equal(t, "default", alters[0].Pool)
	assert.Equal(t, "FT.DROPINDEX test DD", alters[0].Query)
	assert.True(t, alters[0].Safe)
	alters[0].Execute()
	alters = engine.GetRedisSearchIndexAlters()
	assert.Len(t, alters, 0)
}
