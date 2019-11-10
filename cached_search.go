package orm

//import (
//	"fmt"
//	_ "github.com/go-sql-driver/mysql"
//	"math"
//	"strconv"
//	"strings"
//)
//
//func CachedSearch(entityName string, indexName string, pager Pager, arguments ...interface{}) (results []interface{}, totalRows int) {
//
//	schema := GetTableSchema(entityName)
//	definition, has := schema.cachedIndexes[indexName]
//	if !has {
//		panic(fmt.Errorf("uknown index %s", indexName))
//	}
//	start := (pager.GetCurrentPage() - 1) * pager.GetPageSize()
//	if start+pager.GetPageSize() > definition.Max {
//		panic(fmt.Errorf("max cache index page size exceeded %s", indexName))
//	}
//
//	Where := NewWhere(definition.Query, arguments...)
//	localCache := schema.GetLocalCacheContainer()
//	contextCache := getContextCache()
//	if localCache == nil && contextCache != nil {
//		localCache = contextCache
//	}
//	redisCache := schema.GetRedisCacheContainer()
//	var cacheKey string
//
//	cacheKey = schema.getCacheKeySearch(indexName, Where.GetParameters()...)
//	end := pager.GetPageSize()
//	if start+end > definition.Max {
//		end = totalRows - start
//	}
//	const idsOnCachePage = 1000
//
//	minCachePage := float64((pager.GetCurrentPage() - 1) * pager.GetPageSize() / idsOnCachePage)
//	minCachePageCeil := math.Ceil(minCachePage)
//	maxCachePage := float64((pager.GetCurrentPage()-1)*pager.GetPageSize()+pager.GetPageSize()) / float64(idsOnCachePage)
//	maxCachePageCeil := math.Ceil(maxCachePage)
//	pages := make([]string, 0)
//	filledPages := make(map[string][]uint64)
//	for i := minCachePageCeil; i < maxCachePageCeil; i++ {
//		pages = append(pages, strconv.Itoa(int(i)+1))
//	}
//	var fromCache map[string]interface{}
//	var nilsKeys []string
//	if localCache != nil {
//		lenPages := len(pages)
//		fromCache = make(map[string]interface{}, lenPages)
//		nils := make(map[int]int)
//		nilsKeys = make([]string, 0)
//		i := 0
//		fromCache = localCache.HMget(cacheKey, pages...)
//		index := 0
//		for key, val := range fromCache {
//			if val == nil {
//				nils[index] = i
//				i++
//				nilsKeys = append(nilsKeys, key)
//			}
//		}
//		if redisCache != nil && len(nilsKeys) > 0 {
//			fromRedis := redisCache.HMget(cacheKey, nilsKeys...)
//			for key, idsFromRedis := range fromRedis {
//				fromCache[key] = idsFromRedis
//			}
//		}
//	} else if redisCache != nil {
//		fromCache = redisCache.HMget(cacheKey, pages...)
//	}
//	hasNil := false
//	totalRows = 0
//	minPage := 9999
//	maxPage := 0
//	for key, idsAsString := range fromCache {
//		if idsAsString == nil {
//			hasNil = true
//			p, _ := strconv.Atoi(key)
//			if p < minPage {
//				minPage = p
//			}
//			if p > maxPage {
//				maxPage = p
//			}
//		} else {
//			ids := strings.Split(idsAsString.(string), " ")
//			totalRows, _ = strconv.Atoi(ids[0])
//			length := len(ids)
//			idsAsUint := make([]uint64, length-1)
//			for i := 1; i < length; i++ {
//				idsAsUint[i-1], _ = strconv.ParseUint(ids[i], 10, 64)
//			}
//			filledPages[key] = idsAsUint
//		}
//	}
//
//	if hasNil {
//		searchPager := NewPager(minPage, maxPage*idsOnCachePage)
//		results, total := SearchIdsWithCount(Where, searchPager, entityName)
//		totalRows = total
//		cacheFields := make(map[string]interface{})
//		for key, ids := range fromCache {
//			if ids == nil {
//				page := key
//				pageInt, _ := strconv.Atoi(page)
//				sliceStart := (pageInt - minPage) * idsOnCachePage
//				if sliceStart > total {
//					cacheFields[page] = total
//					continue
//				}
//				sliceEnd := sliceStart + idsOnCachePage
//				if sliceEnd > total {
//					sliceEnd = total
//				}
//				values := []uint64{uint64(total)}
//				foundIds := results[sliceStart:sliceEnd]
//				filledPages[key] = foundIds
//				values = append(values, foundIds...)
//				cacheValue := fmt.Sprintf("%v", values)
//				cacheValue = strings.Trim(cacheValue, "[]")
//				cacheFields[page] = cacheValue
//			}
//		}
//		if redisCache != nil {
//			redisCache.HMset(cacheKey, cacheFields)
//		}
//	}
//
//	nilKeysLen := len(nilsKeys)
//	if localCache != nil && nilKeysLen > 0 {
//		fields := make(map[string]interface{}, nilKeysLen)
//		for _, v := range nilsKeys {
//			values := []uint64{uint64(totalRows)}
//			values = append(values, filledPages[v]...)
//			cacheValue := fmt.Sprintf("%v", values)
//			cacheValue = strings.Trim(cacheValue, "[]")
//			fields[v] = cacheValue
//		}
//		localCache.HMset(cacheKey, fields)
//	}
//
//	resultsIds := make([]uint64, 0, len(filledPages)*idsOnCachePage)
//	for i := minCachePageCeil; i < maxCachePageCeil; i++ {
//		resultsIds = append(resultsIds, filledPages[strconv.Itoa(int(i)+1)]...)
//	}
//	sliceStart := (pager.GetCurrentPage() - 1) * pager.GetPageSize()
//	diff := int(minCachePageCeil) * idsOnCachePage
//	sliceStart -= diff
//	sliceEnd := sliceStart + pager.GetPageSize()
//	length := len(resultsIds)
//	if sliceEnd > length {
//		sliceEnd = length
//	}
//	idsToReturn := resultsIds[sliceStart:sliceEnd]
//	results = GetByIds(idsToReturn, entityName)
//	return
//}
