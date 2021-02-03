package orm

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"

	"github.com/segmentio/fasthash/fnv1a"

	"github.com/juju/errors"
)

const idsOnCachePage = 100

func cachedSearch(engine *Engine, entities interface{}, indexName string, pager *Pager,
	arguments []interface{}, references []string) (totalRows int, ids []uint64) {
	value := reflect.ValueOf(entities)
	entityType, has, name := getEntityTypeForSlice(engine.registry, value.Type())
	if !has {
		panic(fmt.Errorf("entity '%s' is not registered", name))
	}
	schema := getTableSchema(engine.registry, entityType)
	definition, has := schema.cachedIndexes[indexName]
	if !has {
		panic(errors.NotFoundf("index %s", indexName))
	}
	if pager == nil {
		pager = NewPager(1, definition.Max)
	}
	start := (pager.GetCurrentPage() - 1) * pager.GetPageSize()
	if start+pager.GetPageSize() > definition.Max {
		panic(errors.Errorf("max cache index page size (%d) exceeded %s", definition.Max, indexName))
	}
	localCache, hasLocalCache := schema.GetLocalCache(engine)
	if !hasLocalCache && engine.hasRequestCache {
		hasLocalCache = true
		localCache = engine.GetLocalCache(requestCacheKey)
	}
	redisCache, hasRedis := schema.GetRedisCache(engine)
	if !hasLocalCache && !hasRedis {
		panic(errors.Errorf("cache search not allowed for entity without cache: '%s'", entityType.String()))
	}
	where := NewWhere(definition.Query, arguments...)
	cacheKey := getCacheKeySearch(schema, indexName, where.GetParameters()...)

	minCachePage := float64((pager.GetCurrentPage() - 1) * pager.GetPageSize() / idsOnCachePage)
	minCachePageCeil := minCachePage
	maxCachePage := float64((pager.GetCurrentPage()-1)*pager.GetPageSize()+pager.GetPageSize()) / float64(idsOnCachePage)
	maxCachePageCeil := math.Ceil(maxCachePage)
	pages := make([]string, int(maxCachePageCeil-minCachePageCeil))
	j := 0
	for i := minCachePageCeil; i < maxCachePageCeil; i++ {
		pages[j] = strconv.Itoa(int(i) + 1)
		j++
	}
	filledPages := make(map[string][]uint64)
	fromRedis := false
	var fromCache map[string]interface{}
	var nilsKeys []string
	if hasLocalCache {
		nilsKeys = make([]string, 0)
		fromCache = localCache.HMget(cacheKey, pages...)
		for key, val := range fromCache {
			if val == nil {
				nilsKeys = append(nilsKeys, key)
			}
		}
		if hasRedis && len(nilsKeys) > 0 {
			fromRedis := redisCache.HMget(cacheKey, nilsKeys...)
			for key, idsFromRedis := range fromRedis {
				fromCache[key] = idsFromRedis
			}
		}
	} else if hasRedis {
		fromRedis = true
		fromCache = redisCache.HMget(cacheKey, pages...)
	}
	hasNil := false
	totalRows = 0
	minPage := 9999
	maxPage := 0
	for key, idsSlice := range fromCache {
		if idsSlice == nil {
			hasNil = true
			p, _ := strconv.Atoi(key)
			if p < minPage {
				minPage = p
			}
			if p > maxPage {
				maxPage = p
			}
		} else {
			if fromRedis {
				ids := strings.Split(idsSlice.(string), " ")
				totalRows, _ = strconv.Atoi(ids[0])
				length := len(ids)
				idsAsUint := make([]uint64, length-1)
				for i := 1; i < length; i++ {
					idsAsUint[i-1], _ = strconv.ParseUint(ids[i], 10, 64)
				}
				filledPages[key] = idsAsUint
			} else {
				ids := idsSlice.([]uint64)
				totalRows = int(ids[0])
				filledPages[key] = ids[1:]
			}
		}
	}

	if hasNil {
		searchPager := NewPager(minPage, maxPage*idsOnCachePage)
		results, total := searchIDsWithCount(false, engine, where, searchPager, entityType)
		totalRows = total
		cacheFields := make(map[string]interface{})
		for key, ids := range fromCache {
			if ids == nil {
				page := key
				pageInt, _ := strconv.Atoi(page)
				sliceStart := (pageInt - minPage) * idsOnCachePage
				if sliceStart > total {
					cacheFields[page] = total
					continue
				}
				sliceEnd := sliceStart + idsOnCachePage
				if sliceEnd > total {
					sliceEnd = total
				}
				l := len(results)
				if sliceEnd > l {
					sliceEnd = l
				}
				values := []uint64{uint64(total)}
				foundIDs := results[sliceStart:sliceEnd]
				filledPages[key] = foundIDs
				values = append(values, foundIDs...)
				cacheValue := fmt.Sprintf("%v", values)
				cacheValue = strings.Trim(cacheValue, "[]")
				cacheFields[page] = cacheValue
			}
		}
		if hasRedis {
			redisCache.HMset(cacheKey, cacheFields)
		}
	}
	nilKeysLen := len(nilsKeys)
	if hasLocalCache && nilKeysLen > 0 {
		fields := make(map[string]interface{}, nilKeysLen)
		for _, v := range nilsKeys {
			values := []uint64{uint64(totalRows)}
			values = append(values, filledPages[v]...)
			fields[v] = values
		}
		localCache.HMset(cacheKey, fields)
	}

	resultsIDs := make([]uint64, 0)
	for i := minCachePageCeil; i < maxCachePageCeil; i++ {
		resultsIDs = append(resultsIDs, filledPages[strconv.Itoa(int(i)+1)]...)
	}
	sliceStart := (pager.GetCurrentPage() - 1) * pager.GetPageSize()
	diff := int(minCachePageCeil) * idsOnCachePage
	sliceStart -= diff
	sliceEnd := sliceStart + pager.GetPageSize()
	length := len(resultsIDs)
	if sliceEnd > length {
		sliceEnd = length
	}
	idsToReturn := resultsIDs[sliceStart:sliceEnd]
	_, is := entities.(Entity)
	if !is {
		tryByIDs(engine, idsToReturn, value.Elem(), references)
	}
	return totalRows, idsToReturn
}

func cachedSearchOne(engine *Engine, entity Entity, indexName string, arguments []interface{}, references []string) (has bool) {
	value := reflect.ValueOf(entity)
	entityType := value.Elem().Type()
	schema := getTableSchema(engine.registry, entityType)
	if schema == nil {
		panic(fmt.Errorf("entity '%s' is not registered", entityType.String()))
	}
	definition, has := schema.cachedIndexesOne[indexName]
	if !has {
		panic(errors.NotFoundf("index %s", indexName))
	}
	Where := NewWhere(definition.Query, arguments...)
	localCache, hasLocalCache := schema.GetLocalCache(engine)
	if !hasLocalCache && engine.hasRequestCache {
		hasLocalCache = true
		localCache = engine.GetLocalCache(requestCacheKey)
	}
	redisCache, hasRedis := schema.GetRedisCache(engine)
	if !hasLocalCache && !hasRedis {
		panic(errors.Errorf("cache search not allowed for entity without cache: '%s'", entityType.String()))
	}
	cacheKey := getCacheKeySearch(schema, indexName, Where.GetParameters()...)
	var fromCache map[string]interface{}
	if hasLocalCache {
		fromCache = localCache.HMget(cacheKey, "1")
	}
	if fromCache["1"] == nil && hasRedis {
		fromCache = redisCache.HMget(cacheKey, "1")
	}
	var id uint64
	if fromCache["1"] == nil {
		results, _ := searchIDs(true, engine, Where, NewPager(1, 1), false, entityType)
		l := len(results)
		value := strconv.Itoa(l)
		if l > 0 {
			id = results[0]
			value += " " + strconv.FormatUint(results[0], 10)
		}
		fields := map[string]interface{}{"1": value}
		if hasLocalCache {
			localCache.HMset(cacheKey, fields)
		}
		if hasRedis {
			redisCache.HMset(cacheKey, fields)
		}
	} else {
		ids := strings.Split(fromCache["1"].(string), " ")
		if ids[0] != "0" {
			id, _ = strconv.ParseUint(ids[1], 10, 64)
		}
	}
	if id > 0 {
		return engine.LoadByID(id, entity, references...)
	}
	return false
}

func getCacheKeySearch(tableSchema *tableSchema, indexName string, parameters ...interface{}) string {
	return tableSchema.cachePrefix + "_" + indexName + strconv.Itoa(int(fnv1a.HashString32(fmt.Sprintf("%v", parameters))))
}
