package orm

import (
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"math"
	"reflect"
	"strconv"
	"strings"
)

func CachedSearch(entityName string, indexName string, pager Pager, arguments ...interface{}) (results []interface{}, totalRows int) {

	schema := GetTableSchema(entityName)
	definition, has := schema.cachedIndexes[indexName]
	if !has {
		panic(fmt.Errorf("uknown index %s", indexName))
	}
	start := (pager.GetCurrentPage() - 1) * pager.GetPageSize()
	if start+pager.GetPageSize() > definition.Max {
		panic(fmt.Errorf("max cache index page size exceeded %s", indexName))
	}

	Where := NewWhere(definition.Query, arguments...)
	localCache := schema.GetLocalCacheContainer()
	redisCache := schema.GetRedisCacheContainer()
	var cacheKey string

	if localCache != nil {
		cacheKey = schema.getCacheKeySearch(indexName, Where.GetParameters()...)
		fromCache, has := localCache.Get(cacheKey)
		if has {
			ids := fromCache.([]uint64)
			totalRows = len(ids)
			if start > totalRows {
				return make([]interface{}, 0), totalRows
			}
			sliceEnd := start + pager.GetPageSize()
			if sliceEnd > totalRows {
				sliceEnd = totalRows
			}
			slice := ids[start:sliceEnd]
			results = GetByIds(slice, entityName)
			return
		}
	} else if redisCache != nil {

		contextCache := getContentCache()

		cacheKey = schema.getCacheKeySearch(indexName, Where.GetParameters()...)
		end := pager.GetPageSize()
		if start+end > definition.Max {
			end = totalRows - start
		}
		const idsOnCachePage = 1000

		minCachePage := float64((pager.GetCurrentPage() - 1) * pager.GetPageSize() / idsOnCachePage)
		minCachePageCeil := math.Ceil(minCachePage)
		maxCachePage := float64((pager.GetCurrentPage()-1)*pager.GetPageSize()+pager.GetPageSize()) / float64(idsOnCachePage)
		maxCachePageCeil := math.Ceil(maxCachePage)
		pages := make([]string, 0)
		filledPages := make(map[string][]uint64)
		for i := minCachePageCeil; i < maxCachePageCeil; i++ {
			pages = append(pages, strconv.Itoa(int(i)+1))
		}
		var fromCache map[string]interface{}
		var nilsKeys []string
		if contextCache != nil {
			lenPages := len(pages)
			fromCache = make(map[string]interface{}, lenPages)
			nils := make(map[int]int)
			nilsKeys = make([]string, 0)
			i := 0
			keys := make([]string, lenPages)
			keysMap := make(map[string]string, lenPages)
			for index, page := range pages {
				newCacheKey := cacheKey + ":" + page
				keys[index] = newCacheKey
				keysMap[newCacheKey] = page
			}
			fromCacheContext := contextCache.MGet(keys...)
			index := 0
			for key, val := range fromCacheContext {
				if val == nil {
					nils[index] = i
					i++
					nilsKeys = append(nilsKeys, keysMap[key])
				}
				fromCache[keysMap[key]] = val
			}
			if len(nilsKeys) > 0 {
				fromRedis := redisCache.HMget(cacheKey, nilsKeys...)
				for key, idsFromRedis := range fromRedis {
					fromCache[key] = idsFromRedis
				}
			}
		} else {
			fromCache = redisCache.HMget(cacheKey, pages...)
		}
		hasNil := false
		totalRows = 0
		for key, idsAsString := range fromCache {
			if idsAsString == nil {
				hasNil = true
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
			searchPager := NewPager(1, definition.Max)
			results, total := SearchIdsWithCount(Where, searchPager, entityName)
			totalRows = total
			cacheFields := make(map[string]interface{})
			for key, ids := range fromCache {
				if ids == nil {
					page := key
					pageInt, _ := strconv.Atoi(page)
					sliceStart := (pageInt - 1) * idsOnCachePage
					if sliceStart > total {
						cacheFields[page] = total
						continue
					}
					sliceEnd := sliceStart + idsOnCachePage
					if sliceEnd > total {
						sliceEnd = total
					}
					values := []uint64{uint64(total)}
					foundIds := results[sliceStart:sliceEnd]
					filledPages[key] = foundIds
					values = append(values, foundIds...)
					cacheValue := fmt.Sprintf("%v", values)
					cacheValue = strings.Trim(cacheValue, "[]")
					cacheFields[page] = cacheValue
				}
			}
			redisCache.HMset(cacheKey, cacheFields)
		}

		nilKeysLen := len(nilsKeys)
		if contextCache != nil && nilKeysLen > 0 {
			pairs := make([]interface{}, nilKeysLen*2)
			i := 0
			for _, v := range nilsKeys {
				pairs[i] = cacheKey + ":" + v
				cacheValue := fmt.Sprintf("%v", filledPages[v])
				cacheValue = strings.Trim(cacheValue, "[]")
				pairs[i+1] = cacheValue
				i += 2
			}
			contextCache.MSet(pairs...)
		}

		resultsIds := make([]uint64, 0, len(filledPages)*idsOnCachePage)
		for i := minCachePageCeil; i < maxCachePageCeil; i++ {
			resultsIds = append(resultsIds, filledPages[strconv.Itoa(int(i)+1)]...)
		}
		sliceStart := (pager.GetCurrentPage() - 1) * pager.GetPageSize()
		diff := int(minCachePageCeil) * idsOnCachePage
		sliceStart -= diff
		sliceEnd := sliceStart + pager.GetPageSize()
		length := len(resultsIds)
		if sliceEnd > length {
			sliceEnd = length
		}
		idsToReturn := resultsIds[sliceStart:sliceEnd]
		results = GetByIds(idsToReturn, entityName)
		return
	} else {
		panic(fmt.Errorf("cache not defined %s", entityName))
	}
	searchPager := NewPager(1, definition.Max)
	results = Search(Where, searchPager, entityName)
	totalRows = len(results)
	ids := make([]uint64, totalRows)
	for k, v := range results {
		ids[k] = reflect.ValueOf(v).Field(1).Uint()
	}
	localCache.Set(cacheKey, ids)
	return
}
