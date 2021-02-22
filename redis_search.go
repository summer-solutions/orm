package orm

import (
	"context"
	"fmt"
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
	DefaultScore    float64
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

func (rs *RedisSearchIndex) AddTextField(name string, weight float64, sortable, noindex, nostem bool) {
	rs.Fields = append(rs.Fields, RedisSearchIndexField{
		Type:     redisSearchIndexFieldText,
		Name:     name,
		Sortable: sortable,
		NoIndex:  noindex,
		NoStem:   nostem,
		Weight:   weight,
	})
}

func (rs *RedisSearchIndex) AddNumericField(name string, sortable, noindex bool) {
	rs.Fields = append(rs.Fields, RedisSearchIndexField{
		Type:     redisSearchIndexFieldNumeric,
		Name:     name,
		Sortable: sortable,
		NoIndex:  noindex,
	})
}

func (rs *RedisSearchIndex) AddGeoField(name string, sortable, noindex bool) {
	rs.Fields = append(rs.Fields, RedisSearchIndexField{
		Type:     redisSearchIndexFieldGeo,
		Name:     name,
		Sortable: sortable,
		NoIndex:  noindex,
	})
}

func (rs *RedisSearchIndex) AddTagField(name string, sortable, noindex bool, separator string) {
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
	Weight       float64
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
	RecordsPerDocAvg         int
	BytesPerRecordAvg        int
	OffsetsPerTermAvg        float64
	OffsetBitsPerRecordAvg   float64
	HashIndexingFailures     uint64
	Indexing                 bool
	PercentIndexed           float64
	StopWords                []string
}

type RedisSearchIndexInfoDefinition struct {
	KeyType       string
	Prefixes      []string
	LanguageField string
	ScoreField    string
	PayloadField  string
	DefaultScore  float64
}

type RedisSearchIndexInfoField struct {
	Name         string
	Type         string
	Weight       float64
	Sortable     bool
	NoSteam      bool
	NoIndex      bool
	TagSeparator string
}

type RedisSearchQuery struct {
	query              string
	filtersInt         map[string][]int64
	filtersFloat       map[string][]float64
	filtersGeo         map[string][]interface{}
	inKeys             []interface{}
	inFields           []interface{}
	toReturn           []interface{}
	sortDesc           bool
	sortField          string
	verbatim           bool
	noStopWords        bool
	withScores         bool
	withPayLoads       bool
	slop               int
	inOrder            bool
	lang               string
	explainScore       bool
	highlight          []interface{}
	highlightOpenTag   string
	highlightCloseTag  string
	summarize          []interface{}
	summarizeSeparator string
	summarizeFrags     int
	summarizeLen       int
}

type RedisSearchResult struct {
	Key          string
	Fields       []interface{}
	Score        float64
	ExplainScore []interface{}
	PayLoad      string
}

func (r *RedisSearchResult) Value(field string) interface{} {
	for i := 0; i < len(r.Fields); i += 2 {
		if r.Fields[i] == field {
			return r.Fields[i+1]
		}
	}
	return nil
}

func (q *RedisSearchQuery) Query(query string) *RedisSearchQuery {
	q.query = query
	return q
}

func (q *RedisSearchQuery) FilterInt(field string, min, max int64) *RedisSearchQuery {
	if q.filtersInt == nil {
		q.filtersInt = make(map[string][]int64)
	}
	q.filtersInt[field] = []int64{min, max}
	return q
}

func (q *RedisSearchQuery) FilterFloat(field string, min, max float64) *RedisSearchQuery {
	if q.filtersFloat == nil {
		q.filtersFloat = make(map[string][]float64)
	}
	q.filtersFloat[field] = []float64{min, max}
	return q
}

func (q *RedisSearchQuery) FilterGeo(field string, lon, lat, radius float64, unit string) *RedisSearchQuery {
	if q.filtersGeo == nil {
		q.filtersGeo = make(map[string][]interface{})
	}
	q.filtersGeo[field] = []interface{}{lon, lat, radius, unit}
	return q
}

func (q *RedisSearchQuery) Sort(field string, desc bool) *RedisSearchQuery {
	q.sortField = field
	q.sortDesc = desc
	return q
}

func (q *RedisSearchQuery) Verbatim() *RedisSearchQuery {
	q.verbatim = true
	return q
}

func (q *RedisSearchQuery) NoStopWords() *RedisSearchQuery {
	q.noStopWords = true
	return q
}

func (q *RedisSearchQuery) WithScores() *RedisSearchQuery {
	q.withScores = true
	return q
}

func (q *RedisSearchQuery) WithPayLoads() *RedisSearchQuery {
	q.withPayLoads = true
	return q
}

func (q *RedisSearchQuery) InKeys(key ...string) *RedisSearchQuery {
	for _, k := range key {
		q.inKeys = append(q.inKeys, k)
	}
	return q
}

func (q *RedisSearchQuery) InFields(field ...string) *RedisSearchQuery {
	for _, k := range field {
		q.inFields = append(q.inFields, k)
	}
	return q
}

func (q *RedisSearchQuery) Return(field ...string) *RedisSearchQuery {
	for _, k := range field {
		q.toReturn = append(q.toReturn, k)
	}
	return q
}

func (q *RedisSearchQuery) Slop(slop int) *RedisSearchQuery {
	q.slop = slop
	if q.slop == 0 {
		q.slop = -1
	}
	return q
}

func (q *RedisSearchQuery) InOrder() *RedisSearchQuery {
	q.inOrder = true
	return q
}

func (q *RedisSearchQuery) ExplainScore() *RedisSearchQuery {
	q.explainScore = true
	return q
}

func (q *RedisSearchQuery) Lang(lang string) *RedisSearchQuery {
	q.lang = lang
	return q
}

func (q *RedisSearchQuery) Highlight(field ...string) *RedisSearchQuery {
	if q.highlight == nil {
		q.highlight = make([]interface{}, 0)
	}
	for _, k := range field {
		q.highlight = append(q.highlight, k)
	}
	return q
}

func (q *RedisSearchQuery) HighlightTags(openTag, closeTag string) *RedisSearchQuery {
	q.highlightOpenTag = openTag
	q.highlightCloseTag = closeTag
	return q
}

func (q *RedisSearchQuery) Summarize(field ...string) *RedisSearchQuery {
	if q.summarize == nil {
		q.summarize = make([]interface{}, 0)
	}
	for _, k := range field {
		q.summarize = append(q.summarize, k)
	}
	return q
}

func (q *RedisSearchQuery) SummarizeOptions(separator string, frags, len int) *RedisSearchQuery {
	q.summarizeSeparator = separator
	q.summarizeFrags = frags
	q.summarizeLen = len
	return q
}

func (r *RedisSearch) SearchRaw(index string, query *RedisSearchQuery, pager *Pager) (total int64, rows []interface{}) {
	return r.search(index, query, pager, false)
}

func (r *RedisSearch) Search(index string, query *RedisSearchQuery, pager *Pager) (total int64, rows []*RedisSearchResult) {
	total, data := r.search(index, query, pager, false)
	rows = make([]*RedisSearchResult, 0)
	max := len(data) - 1
	i := 0
	for {
		if i > max {
			break
		}
		row := &RedisSearchResult{Key: data[i].(string)}
		if query.explainScore {
			i++
			row.ExplainScore = data[i].([]interface{})
			row.Score, _ = strconv.ParseFloat(row.ExplainScore[0].(string), 64)
			row.ExplainScore = row.ExplainScore[1].([]interface{})
		} else if query.withScores {
			i++
			row.Score, _ = strconv.ParseFloat(data[i].(string), 64)
		}
		if query.withPayLoads {
			i++
			if data[i] != nil {
				row.PayLoad = data[i].(string)
			}
		}
		i++
		row.Fields = data[i].([]interface{})
		rows = append(rows, row)
		i++
	}

	return total, rows
}

func (r *RedisSearch) SearchKeys(index string, query *RedisSearchQuery, pager *Pager) (total int64, keys []string) {
	total, rows := r.search(index, query, pager, true)
	keys = make([]string, len(rows))
	for k, v := range rows {
		keys[k] = v.(string)
	}
	return total, keys
}

func (r *RedisSearch) search(index string, query *RedisSearchQuery, pager *Pager, noContent bool) (total int64, rows []interface{}) {
	args := []interface{}{"FT.SEARCH", index}
	if query.query != "" {
		args = append(args, query.query)
	} else {
		args = append(args, "*")
	}
	for field, ranges := range query.filtersInt {
		args = append(args, "FILTER", field, ranges[0], ranges[1])
	}
	for field, ranges := range query.filtersFloat {
		args = append(args, "FILTER", field, ranges[0], ranges[1])
	}
	for field, data := range query.filtersGeo {
		args = append(args, "GEOFILTER", field, data[0], data[1], data[2], data[3])
	}
	if noContent {
		args = append(args, "NOCONTENT")
	}
	if query.verbatim {
		args = append(args, "VERBATIM")
	}
	if query.noStopWords {
		args = append(args, "NOSTOPWORDS")
	}
	if query.withScores {
		args = append(args, "WITHSCORES")
	}
	if query.withPayLoads {
		args = append(args, "WITHPAYLOADS")
	}
	if query.sortField != "" {
		args = append(args, "SORTBY", query.sortField)
		if query.sortDesc {
			args = append(args, "DESC")
		}
	}
	if len(query.inKeys) > 0 {
		args = append(args, "INKEYS", len(query.inKeys))
		args = append(args, query.inKeys...)
	}
	if len(query.inFields) > 0 {
		args = append(args, "INFIELDS", len(query.inFields))
		args = append(args, query.inFields...)
	}
	if len(query.toReturn) > 0 {
		args = append(args, "RETURN", len(query.toReturn))
		args = append(args, query.toReturn...)
	}
	if query.slop != 0 {
		slop := query.slop
		if slop == -1 {
			slop = 0
		}
		args = append(args, "SLOP", slop)
	}
	if query.inOrder {
		args = append(args, "INORDER")
	}
	if query.lang != "" {
		args = append(args, "LANGUAGE", query.lang)
	}
	if query.explainScore {
		args = append(args, "EXPLAINSCORE")
	}
	if query.highlight != nil {
		args = append(args, "HIGHLIGHT")
		if l := len(query.highlight); l > 0 {
			args = append(args, "FIELDS", l)
			args = append(args, query.highlight...)
		}
		if query.highlightOpenTag != "" && query.highlightCloseTag != "" {
			args = append(args, "TAGS", query.highlightOpenTag, query.highlightCloseTag)
		}
	}
	if query.summarize != nil {
		args = append(args, "SUMMARIZE")
		if l := len(query.summarize); l > 0 {
			args = append(args, "FIELDS", l)
			args = append(args, query.summarize...)
		}
		if query.summarizeFrags > 0 {
			args = append(args, "FRAGS", query.summarizeFrags)
		}
		if query.summarizeLen > 0 {
			args = append(args, "LEN", query.summarizeLen)
		}
		if query.summarizeSeparator != "" {
			args = append(args, "SEPARATOR", query.summarizeSeparator)
		}
	}
	if pager != nil {
		args = append(args, "LIMIT")
		args = append(args, (pager.CurrentPage-1)*pager.PageSize)
		args = append(args, pager.PageSize)
	}
	cmd := redis.NewSliceCmd(r.ctx, args...)
	start := time.Now()
	err := r.client.Process(r.ctx, cmd)
	if r.engine.hasRedisLogger {
		r.fillLogFields("[ORM][REDIS-SEARCH][FT.SEARCH]", start, "ft_search", 1,
			map[string]interface{}{"Index": index, "args": args[2:]}, err)
	}
	checkError(err)
	res, err := cmd.Result()
	checkError(err)
	total = res[0].(int64)
	return total, res[1:]
}

func (r *RedisSearch) createIndexArgs(index *RedisSearchIndex) []interface{} {
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
	return args
}

func (r *RedisSearch) aliasUpdate(name, index string) {
	cmd := redis.NewStringCmd(r.ctx, "FT.ALIASUPDATE", name, index)
	err := r.client.Process(r.ctx, cmd)
	checkError(err)
}

func (r *RedisSearch) createIndex(index *RedisSearchIndex) string {
	args := r.createIndexArgs(index)
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
					score, _ := strconv.ParseFloat(def[subKey+1].(string), 64)
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
						weight, _ := strconv.ParseFloat(def[subKey+1].(string), 64)
						field.Weight = weight
					case "SORTABLE":
						field.Sortable = true
					case "NOSTEM":
						field.NoSteam = true
					case "NOINDEX":
						field.NoIndex = true
					case "SEPARATOR":
						field.TagSeparator = def[subKey+1].(string)
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
				info.RecordsPerDocAvg, _ = strconv.Atoi(res[i+1].(string))
			}
		case "bytes_per_record_avg":
			if res[i+1] != "-nan" {
				info.BytesPerRecordAvg, _ = strconv.Atoi(res[i+1].(string))
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
			info.Indexing = res[i+1] == "1"
		case "percent_indexed":
			v, _ := strconv.ParseFloat(res[i+1].(string), 64)
			info.PercentIndexed = v
		case "stopwords_list":
			v := res[i+1].([]interface{})
			info.StopWords = make([]string, len(v))
			for i, v := range v {
				info.StopWords[i] = v.(string)
			}
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
		inRedis := make(map[string]bool)
		for _, name := range search.listIndices() {
			_, has := engine.registry.redisSearchIndexes[poolName][name]
			if !has {
				query := "FT.DROPINDEX " + name + " DD"
				alter := RedisSearchIndexAlter{Pool: poolName, Query: query, search: search}
				info := search.info(name)
				alter.Safe = info.NumDocs == 0 && !info.Indexing
				nameToRemove := name
				alter.Execute = func() {
					alter.search.dropIndex(nameToRemove)
				}
				alters = append(alters, alter)
				continue
			}
			inRedis[name] = true
			// TODO alter
		}
		for name, index := range engine.registry.redisSearchIndexes[poolName] {
			_, has := inRedis[name]
			if has {
				continue
			}
			query := fmt.Sprintf("%v", search.createIndexArgs(index))[1:]
			query = query[0 : len(query)-1]
			alter := RedisSearchIndexAlter{Pool: poolName, Query: query, Safe: true, search: search}
			indexToAdd := index
			alter.Execute = func() {
				alter.search.createIndex(indexToAdd)
			}
			alters = append(alters, alter)
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
