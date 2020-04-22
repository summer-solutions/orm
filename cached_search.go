package orm

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
)

func cachedSearch(engine *Engine, entities interface{}, indexName string, pager *Pager,
	arguments []interface{}, references []string) (totalRows int, err error) {
	value := reflect.ValueOf(entities)
	entityType, has := getEntityTypeForSlice(engine.registry, value.Type())
	if !has {
		return 0, EntityNotRegisteredError{Name: entityType.String()}
	}
	schema := getTableSchema(engine.registry, entityType)
	definition, has := schema.cachedIndexes[indexName]
	if !has {
		return 0, fmt.Errorf("uknown index %s", indexName)
	}
	if pager == nil {
		pager = &Pager{CurrentPage: 1, PageSize: definition.Max}
	}
	start := (pager.GetCurrentPage() - 1) * pager.GetPageSize()
	if start+pager.GetPageSize() > definition.Max {
		return 0, fmt.Errorf("max cache index page size (%d) exceeded %s", definition.Max, indexName)
	}

	Where := NewWhere(definition.Query, arguments...)
	localCache, hasLocalCache := schema.GetLocalCache(engine)
	redisCache, hasRedis := schema.GetRedisCache(engine)
	if !hasLocalCache && !hasRedis {
		return 0, fmt.Errorf("cache search not allowed for entity without cache: '%s'", entityType.String())
	}
	cacheKey := schema.getCacheKeySearch(indexName, Where.GetParameters()...)
	const idsOnCachePage = 1000

	minCachePage := float64((pager.GetCurrentPage() - 1) * pager.GetPageSize() / idsOnCachePage)
	minCachePageCeil := minCachePage
	maxCachePage := float64((pager.GetCurrentPage()-1)*pager.GetPageSize()+pager.GetPageSize()) / float64(idsOnCachePage)
	maxCachePageCeil := math.Ceil(maxCachePage)
	pages := make([]string, 0)
	filledPages := make(map[string][]uint64)
	for i := minCachePageCeil; i < maxCachePageCeil; i++ {
		pages = append(pages, strconv.Itoa(int(i)+1))
	}
	var fromCache map[string]interface{}
	var nilsKeys []string
	if hasLocalCache {
		nils := make(map[int]int)
		nilsKeys = make([]string, 0)
		i := 0
		fromCache = localCache.HMget(cacheKey, pages...)
		index := 0
		for key, val := range fromCache {
			if val == nil {
				nils[index] = i
				i++
				nilsKeys = append(nilsKeys, key)
			}
		}
		if hasRedis && len(nilsKeys) > 0 {
			fromRedis, err := redisCache.HMget(cacheKey, nilsKeys...)
			if err != nil {
				return 0, err
			}
			for key, idsFromRedis := range fromRedis {
				fromCache[key] = idsFromRedis
			}
		}
	} else if hasRedis {
		fromCache, err = redisCache.HMget(cacheKey, pages...)
		if err != nil {
			return 0, err
		}
	}
	hasNil := false
	totalRows = 0
	minPage := 9999
	maxPage := 0
	for key, idsAsString := range fromCache {
		if idsAsString == nil {
			hasNil = true
			p, _ := strconv.Atoi(key)
			if p < minPage {
				minPage = p
			}
			if p > maxPage {
				maxPage = p
			}
		} else {
			ids := strings.Split(idsAsString.(string), " ")
			totalRows, _ = strconv.Atoi(ids[0])
			length := len(ids)
			idsAsUint := make([]uint64, length-1)
			for i := 1; i < length; i++ {
				idsAsUint[i-1], _ = strconv.ParseUint(ids[i], 10, 64)
			}
			filledPages[key] = idsAsUint
		}
	}

	if hasNil {
		searchPager := &Pager{minPage, maxPage * idsOnCachePage}
		results, total, err := searchIDsWithCount(true, engine, Where, searchPager, entityType)
		if err != nil {
			return 0, err
		}
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
			err := redisCache.HMset(cacheKey, cacheFields)
			if err != nil {
				return 0, err
			}
		}
	}

	nilKeysLen := len(nilsKeys)
	if hasLocalCache && nilKeysLen > 0 {
		fields := make(map[string]interface{}, nilKeysLen)
		for _, v := range nilsKeys {
			values := []uint64{uint64(totalRows)}
			values = append(values, filledPages[v]...)
			cacheValue := fmt.Sprintf("%v", values)
			cacheValue = strings.Trim(cacheValue, "[]")
			fields[v] = cacheValue
		}
		localCache.HMset(cacheKey, fields)
	}

	resultsIDs := make([]uint64, 0, len(filledPages)*idsOnCachePage)
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
	nonZero := make([]uint64, 0, len(idsToReturn))
	for _, id := range idsToReturn {
		if id != 0 {
			nonZero = append(nonZero, id)
		}
	}
	missing, err := engine.LoadByIDs(nonZero, entities, references...)
	if err != nil {
		return 0, err
	}
	if len(missing) > 0 {
		if hasLocalCache {
			localCache.Remove(cacheKey)
		}
		if hasRedis {
			err = redisCache.Del(cacheKey)
			if err != nil {
				return 0, err
			}
		}
		return cachedSearch(engine, entities, indexName, pager, arguments, references)
	}
	return
}

func cachedSearchOne(engine *Engine, entity Entity, indexName string, arguments ...interface{}) (has bool, err error) {
	value := reflect.ValueOf(entity)
	entityType := value.Elem().Type()
	schema := getTableSchema(engine.registry, entityType)
	if schema == nil {
		return false, EntityNotRegisteredError{Name: entityType.String()}
	}
	definition, has := schema.cachedIndexesOne[indexName]
	if !has {
		return false, fmt.Errorf("uknown index %s", indexName)
	}
	Where := NewWhere(definition.Query, arguments...)
	localCache, hasLocalCache := schema.GetLocalCache(engine)
	redisCache, hasRedis := schema.GetRedisCache(engine)
	if !hasLocalCache && !hasRedis {
		return false, fmt.Errorf("cache search not allowed for entity without cache: '%s'", entityType.String())
	}
	cacheKey := schema.getCacheKeySearch(indexName, Where.GetParameters()...)
	var fromCache map[string]interface{}
	if hasLocalCache {
		fromCache = localCache.HMget(cacheKey, "1")
	}
	if fromCache["1"] == nil && hasRedis {
		fromCache, err = redisCache.HMget(cacheKey, "1")
		if err != nil {
			return false, err
		}
	}
	var id uint64
	if fromCache["1"] == nil {
		results, _, err := searchIDs(true, engine, Where, &Pager{CurrentPage: 1, PageSize: 1}, false, entityType)
		if err != nil {
			return false, err
		}
		l := len(results)
		value := fmt.Sprintf("%d", l)
		if l > 0 {
			id = results[0]
			value += fmt.Sprintf(" %d", results[0])
		}
		fields := map[string]interface{}{"1": value}
		if hasLocalCache {
			localCache.HMset(cacheKey, fields)
		}
		if hasRedis {
			err := redisCache.HMset(cacheKey, fields)
			if err != nil {
				return false, err
			}
		}
	} else {
		ids := strings.Split(fromCache["1"].(string), " ")
		if ids[0] != "0" {
			id, _ = strconv.ParseUint(ids[1], 10, 64)
		}
	}
	if id > 0 {
		return engine.LoadByID(id, entity)
	}
	return false, nil
}
