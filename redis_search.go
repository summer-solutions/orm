package orm

import (
	"context"
	"strconv"
	"strings"
	"time"

	apexLog "github.com/apex/log"

	"github.com/go-redis/redis/v8"
)

const redisSearchIndexFieldText = "TEXT"
const redisSearchIndexFieldNumeric = "NUMERIC"
const redisSearchIndexFieldGeo = "GEO"
const redisSearchIndexFieldTAG = "TAG"

type RedisSearch struct {
	engine *Engine
	ctx    context.Context
	code   string
	client *redis.Client
}

type RedisSearchIndex struct {
	Name            string
	RedisPool       string
	Prefixes        []string
	Filter          string
	DefaultLanguage string
	LanguageField   string
	DefaultScore    float32
	ScoreField      string
	PayloadField    string
	MaxTextFields   bool
	Temporary       int
	NoOffsets       bool
	NoNHL           bool
	NoFields        bool
	NoFreqs         bool
	SkipInitialScan bool
	StopWords       []string
	Fields          []RedisSearchIndexField
}

func (rs RedisSearchIndex) AddTextField(name string, weight int, sortable, noindex, nostem bool) {
	rs.Fields = append(rs.Fields, RedisSearchIndexField{
		Type:     redisSearchIndexFieldText,
		Name:     name,
		Sortable: sortable,
		NoIndex:  noindex,
		NoStem:   nostem,
		Weight:   weight,
	})
}

func (rs RedisSearchIndex) AddNumericField(name string, sortable, noindex bool) {
	rs.Fields = append(rs.Fields, RedisSearchIndexField{
		Type:     redisSearchIndexFieldNumeric,
		Name:     name,
		Sortable: sortable,
		NoIndex:  noindex,
	})
}

func (rs RedisSearchIndex) AddGeoField(name string, sortable, noindex bool) {
	rs.Fields = append(rs.Fields, RedisSearchIndexField{
		Type:     redisSearchIndexFieldGeo,
		Name:     name,
		Sortable: sortable,
		NoIndex:  noindex,
	})
}

func (rs RedisSearchIndex) AddTagField(name string, sortable, noindex bool, separator string) {
	rs.Fields = append(rs.Fields, RedisSearchIndexField{
		Type:         redisSearchIndexFieldTAG,
		Name:         name,
		Sortable:     sortable,
		NoIndex:      noindex,
		TagSeparator: separator,
	})
}

type RedisSearchIndexField struct {
	Type         string
	Name         string
	Sortable     bool
	NoIndex      bool
	NoStem       bool
	Weight       int
	TagSeparator string
}

type RedisSearchIndexAlter struct {
	search  *RedisSearch
	Query   string
	Safe    bool
	Pool    string
	Execute func()
}

type RedisSearchIndexInfo struct {
	Name                     string
	Options                  []interface{}
	Definition               RedisSearchIndexInfoDefinition
	Fields                   []RedisSearchIndexInfoField
	NumDocs                  uint64
	MaxDocID                 uint64
	NumTerms                 uint64
	NumRecords               uint64
	InvertedSzMB             float64
	TotalInvertedIndexBlocks float64
	OffsetVectorsSzMB        float64
	DocTableSizeMB           float64
	SortableValuesSizeMB     float64
	KeyTableSizeMB           float64
	RecordsPerDocAvg         float64
	BytesPerRecordAvg        float64
	OffsetsPerTermAvg        float64
	OffsetBitsPerRecordAvg   float64
	HashIndexingFailures     uint64
	Indexing                 uint64
	PercentIndexed           float64
}

type RedisSearchIndexInfoDefinition struct {
	KeyType       string
	Prefixes      []string
	LanguageField string
	ScoreField    string
	PayloadField  string
	DefaultScore  int
}

type RedisSearchIndexInfoField struct {
	Name   string
	Type   string
	Weight int
}

func (r *RedisSearch) createIndex(index RedisSearchIndex) string {
	args := []interface{}{"FT.CREATE", index.Name, "ON", "HASH", "PREFIX", len(index.Prefixes)}
	for _, prefix := range index.Prefixes {
		args = append(args, prefix)
	}
	if index.Filter != "" {
		args = append(args, "FILTER", index.Filter)
	}
	if index.DefaultLanguage != "" {
		args = append(args, "LANGUAGE", index.DefaultLanguage)
	}
	if index.LanguageField != "" {
		args = append(args, "LANGUAGE_FIELD", index.LanguageField)
	}
	if index.DefaultScore > 0 {
		args = append(args, "SCORE", index.DefaultScore)
	}
	if index.ScoreField != "" {
		args = append(args, "SCORE_FIELD", index.ScoreField)
	}
	if index.PayloadField != "" {
		args = append(args, "PAYLOAD_FIELD", index.PayloadField)
	}
	if index.MaxTextFields {
		args = append(args, "MAXTEXTFIELDS")
	}
	if index.Temporary > 0 {
		args = append(args, "TEMPORARY", index.Temporary)
	}
	if index.NoOffsets {
		args = append(args, "NOOFFSETS")
	}
	if index.NoNHL {
		args = append(args, "NOHL")
	}
	if index.NoFields {
		args = append(args, "NOFIELDS")
	}
	if index.NoFreqs {
		args = append(args, "NOFREQS")
	}
	if index.SkipInitialScan {
		args = append(args, "SKIPINITIALSCAN")
	}
	if len(index.StopWords) > 0 {
		args = append(args, "STOPWORDS", len(index.StopWords))
		for _, word := range index.StopWords {
			args = append(args, word)
		}
	}
	args = append(args, "SCHEMA")
	for _, field := range index.Fields {
		fieldArgs := []interface{}{field.Name, field.Type}
		if field.Type == redisSearchIndexFieldText {
			if field.NoStem {
				fieldArgs = append(fieldArgs, "NOSTEM")
			}
			if field.Weight != 1 {
				fieldArgs = append(fieldArgs, "WEIGHT", field.Weight)
			}
		} else if field.Type == redisSearchIndexFieldTAG {
			if field.TagSeparator != "" && field.TagSeparator != ", " {
				fieldArgs = append(fieldArgs, "SEPARATOR", field.TagSeparator)
			}
		}
		if field.Sortable {
			fieldArgs = append(fieldArgs, "SORTABLE")
		}
		if field.NoIndex {
			fieldArgs = append(fieldArgs, "NOINDEX")
		}
		args = append(args, fieldArgs...)
	}
	cmd := redis.NewStringCmd(r.ctx, args...)

	start := time.Now()
	err := r.client.Process(r.ctx, cmd)
	if r.engine.hasRedisLogger {
		r.fillLogFields("[ORM][REDIS-SEARCH][FT.CREATE]", start, "ft_create", 1,
			map[string]interface{}{"Index": index.Name}, err)
	}
	checkError(err)
	res, err := cmd.Result()
	checkError(err)
	return res
}

func (r *RedisSearch) listIndices() []string {
	cmd := redis.NewStringSliceCmd(r.ctx, "FT._LIST")
	start := time.Now()
	err := r.client.Process(r.ctx, cmd)
	if r.engine.hasRedisLogger {
		r.fillLogFields("[ORM][REDIS-SEARCH][FT.LIST]", start, "ft_list", 1, nil, err)
	}
	checkError(err)
	res, err := cmd.Result()
	checkError(err)
	return res
}

func (r *RedisSearch) dropIndex(indexName string) string {
	cmd := redis.NewStringCmd(r.ctx, "FT.DROPINDEX", indexName, "DD")
	start := time.Now()
	err := r.client.Process(r.ctx, cmd)
	if r.engine.hasRedisLogger {
		r.fillLogFields("[ORM][REDIS-SEARCH][FT.DROPINDEX]", start, "ft_dropindex", 1,
			apexLog.Fields{"Index": indexName}, err)
	}
	checkError(err)
	res, err := cmd.Result()
	checkError(err)
	return res
}

func (r *RedisSearch) info(indexName string) RedisSearchIndexInfo {
	cmd := redis.NewSliceCmd(r.ctx, "FT.INFO", indexName)
	start := time.Now()
	err := r.client.Process(r.ctx, cmd)
	if r.engine.hasRedisLogger {
		r.fillLogFields("[ORM][REDIS-SEARCH][FT.INFO]", start, "ft_info", 1,
			apexLog.Fields{"Index": indexName}, err)
	}
	checkError(err)
	res, err := cmd.Result()
	checkError(err)
	info := RedisSearchIndexInfo{}
	for i, row := range res {
		switch row {
		case "index_name":
			info.Name = res[i+1].(string)
		case "index_options":
			info.Options = res[i+1].([]interface{})
		case "index_definition":
			def := res[i+1].([]interface{})
			definition := RedisSearchIndexInfoDefinition{}
			for subKey, subValue := range def {
				switch subValue {
				case "key_type":
					definition.KeyType = def[subKey+1].(string)
				case "prefixes":
					prefixesRaw := def[subKey+1].([]interface{})
					prefixes := make([]string, len(prefixesRaw))
					for k, v := range prefixesRaw {
						prefixes[k] = v.(string)
					}
					definition.Prefixes = prefixes
				case "language_field":
					definition.LanguageField = def[subKey+1].(string)
				case "default_score":
					score, _ := strconv.Atoi(def[subKey+1].(string))
					definition.DefaultScore = score
				case "score_field":
					definition.ScoreField = def[subKey+1].(string)
				case "payload_field":
					definition.PayloadField = def[subKey+1].(string)
				}
			}
			info.Definition = definition
		case "fields":
			fieldsRaw := res[i+1].([]interface{})
			fields := make([]RedisSearchIndexInfoField, len(fieldsRaw))
			for i, v := range fieldsRaw {
				def := v.([]interface{})
				field := RedisSearchIndexInfoField{Name: def[0].(string)}
				def = def[1:]
				for subKey, subValue := range def {
					switch subValue {
					case "type":
						field.Type = def[subKey+1].(string)
					case "WEIGHT":
						weight, _ := strconv.Atoi(def[subKey+1].(string))
						field.Weight = weight
					}
				}
				fields[i] = field
			}
			info.Fields = fields
		case "num_docs":
			v, _ := strconv.ParseUint(res[i+1].(string), 10, 64)
			info.NumDocs = v
		case "max_doc_id":
			v, _ := strconv.ParseUint(res[i+1].(string), 10, 64)
			info.MaxDocID = v
		case "num_terms":
			v, _ := strconv.ParseUint(res[i+1].(string), 10, 64)
			info.NumTerms = v
		case "num_records":
			v, _ := strconv.ParseUint(res[i+1].(string), 10, 64)
			info.NumRecords = v
		case "inverted_sz_mb":
			v, _ := strconv.ParseFloat(res[i+1].(string), 64)
			info.InvertedSzMB = v
		case "total_inverted_index_blocks":
			v, _ := strconv.ParseFloat(res[i+1].(string), 64)
			info.TotalInvertedIndexBlocks = v
		case "offset_vectors_sz_mb":
			v, _ := strconv.ParseFloat(res[i+1].(string), 64)
			info.OffsetVectorsSzMB = v
		case "doc_table_size_mb":
			v, _ := strconv.ParseFloat(res[i+1].(string), 64)
			info.DocTableSizeMB = v
		case "sortable_values_size_mb":
			v, _ := strconv.ParseFloat(res[i+1].(string), 64)
			info.SortableValuesSizeMB = v
		case "key_table_size_mb":
			v, _ := strconv.ParseFloat(res[i+1].(string), 64)
			info.KeyTableSizeMB = v
		case "records_per_doc_avg":
			if res[i+1] != "-nan" {
				v, _ := strconv.ParseFloat(res[i+1].(string), 64)
				info.RecordsPerDocAvg = v
			}
		case "bytes_per_record_avg":
			if res[i+1] != "-nan" {
				v, _ := strconv.ParseFloat(res[i+1].(string), 64)
				info.BytesPerRecordAvg = v
			}
		case "offsets_per_term_avg":
			if res[i+1] != "-nan" {
				v, _ := strconv.ParseFloat(res[i+1].(string), 64)
				info.OffsetsPerTermAvg = v
			}
		case "offset_bits_per_record_avg":
			if res[i+1] != "-nan" {
				v, _ := strconv.ParseFloat(res[i+1].(string), 64)
				info.OffsetBitsPerRecordAvg = v
			}
		case "hash_indexing_failures":
			v, _ := strconv.ParseUint(res[i+1].(string), 10, 64)
			info.HashIndexingFailures = v
		case "indexing":
			v, _ := strconv.ParseUint(res[i+1].(string), 10, 64)
			info.Indexing = v
		case "percent_indexed":
			v, _ := strconv.ParseFloat(res[i+1].(string), 64)
			info.PercentIndexed = v
		}
	}
	return info
}

func getRedisSearchAlters(engine *Engine) (alters []RedisSearchIndexAlter) {
	alters = make([]RedisSearchIndexAlter, 0)
	for _, poolName := range engine.GetRegistry().GetRedisPools() {
		r := engine.GetRedis(poolName)
		info := r.Info("Modules")
		lines := strings.Split(info, "\r\n")
		hasModule := false
		for _, line := range lines {
			if strings.HasPrefix(line, "module:name=search") {
				hasModule = true
				break
			}
		}
		if !hasModule {
			continue
		}
		search := engine.GetRedisSearch(poolName)
		for _, name := range search.listIndices() {
			_, has := engine.registry.redisSearchIndexes[name]
			if !has {
				query := "FT.DROPINDEX " + name + " DD"
				alter := RedisSearchIndexAlter{Pool: poolName, Query: query, search: search}
				info := search.info(name)
				alter.Safe = info.NumDocs == 0 && info.Indexing == 0
				alter.Execute = func() {
					alter.search.dropIndex(name)
				}
				alters = append(alters, alter)
			}
		}
	}
	return alters
}

func (r *RedisSearch) fillLogFields(message string, start time.Time, operation string, keys int, fields apexLog.Fields, err error) {
	now := time.Now()
	stop := time.Since(start).Microseconds()
	e := r.engine.queryLoggers[QueryLoggerSourceRedis].log.WithFields(apexLog.Fields{
		"microseconds": stop,
		"operation":    operation,
		"pool":         r.code,
		"keys":         keys,
		"target":       "redis",
		"started":      start.UnixNano(),
		"finished":     now.UnixNano(),
	})
	if fields != nil {
		e = e.WithFields(fields)
	}
	if err != nil {
		injectLogError(err, e).Error(message)
	} else {
		e.Info(message)
	}
}
