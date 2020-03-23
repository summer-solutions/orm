package orm

import (
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
)

func ClearCachedSearchOne(entity interface{}, indexName string, arguments ...interface{}) error {
	_, err := cachedSearchOne(entity, indexName, true, arguments...)
	return err
}

func CachedSearchOne(entity interface{}, indexName string, arguments ...interface{}) (has bool, err error) {
	return cachedSearchOne(entity, indexName, false, arguments...)
}

func ClearCachedSearch(entities interface{}, indexName string, pager *Pager, arguments ...interface{}) (totalRows int, err error) {
	return cachedSearch(entities, indexName, true, pager, arguments...)
}

func CachedSearch(entities interface{}, indexName string, pager *Pager, arguments ...interface{}) (totalRows int, err error) {
	return cachedSearch(entities, indexName, false, pager, arguments...)
}

func cachedSearch(entities interface{}, indexName string, clear bool, pager *Pager, arguments ...interface{}) (totalRows int, err error) {
	value := reflect.ValueOf(entities)
	entityType := getEntityTypeForSlice(value.Type())
	schema := getTableSchema(entityType)
	definition, has := schema.cachedIndexes[indexName]
	if !has {
		return 0, fmt.Errorf("uknown index %s", indexName)
	}
	if pager == nil {
		pager = &Pager{CurrentPage: 1, PageSize: definition.Max}
	}
	start := (pager.GetCurrentPage() - 1) * pager.GetPageSize()
	if start+pager.GetPageSize() > definition.Max {
		return 0, fmt.Errorf("max cache index page size exceeded %s", indexName)
	}

	Where := NewWhere(definition.Query, arguments...)
	localCache := schema.GetLocalCache()
	redisCache := schema.GetRedisCacheContainer()
	if localCache == nil && redisCache == nil {
		return 0, fmt.Errorf("missing entity cache definition")
	}
	var cacheKey string

	cacheKey = schema.getCacheKeySearch(indexName, Where.GetParameters()...)
	fmt.Printf("GETTING %s\n", cacheKey)
	if clear {
		if localCache != nil {
			localCache.Remove(cacheKey)
		}
		if redisCache != nil {
			err := redisCache.Del(cacheKey)
			if err != nil {
				return 0, err
			}
		}
		return 0, nil
	}
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
	if localCache != nil {
		lenPages := len(pages)
		fromCache = make(map[string]interface{}, lenPages)
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
		if redisCache != nil && len(nilsKeys) > 0 {
			fromRedis, err := redisCache.HMget(cacheKey, nilsKeys...)
			if err != nil {
				return 0, err
			}
			for key, idsFromRedis := range fromRedis {
				fromCache[key] = idsFromRedis
			}
		}
	} else if redisCache != nil {
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
		results, total := searchIdsWithCount(Where, searchPager, entityType)
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
				foundIds := results[sliceStart:sliceEnd]
				filledPages[key] = foundIds
				values = append(values, foundIds...)
				cacheValue := fmt.Sprintf("%v", values)
				cacheValue = strings.Trim(cacheValue, "[]")
				cacheFields[page] = cacheValue
			}
		}
		if redisCache != nil {
			err := redisCache.HMset(cacheKey, cacheFields)
			if err != nil {
				return 0, err
			}
		}
	}

	nilKeysLen := len(nilsKeys)
	if localCache != nil && nilKeysLen > 0 {
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
	err = GetByIds(idsToReturn, entities)
	if err != nil {
		return 0, err
	}
	return
}

func cachedSearchOne(entity interface{}, indexName string, clear bool, arguments ...interface{}) (has bool, err error) {
	value := reflect.ValueOf(entity)
	entityType := value.Elem().Type()
	schema := getTableSchema(entityType)
	definition, has := schema.cachedIndexesOne[indexName]
	if !has {
		return false, fmt.Errorf("uknown index %s", indexName)
	}
	Where := NewWhere(definition.Query, arguments...)
	localCache := schema.GetLocalCache()
	redisCache := schema.GetRedisCacheContainer()
	if localCache == nil && redisCache == nil {
		return false, fmt.Errorf("missing entity cache definition")
	}
	cacheKey := schema.getCacheKeySearch(indexName, Where.GetParameters()...)
	if clear {
		if localCache != nil {
			localCache.Remove(cacheKey)
		}
		if redisCache != nil {
			err := redisCache.Del(cacheKey)
			if err != nil {
				return false, err
			}
		}
		return false, nil
	}
	var fromCache map[string]interface{}
	if localCache != nil {
		fromCache = localCache.HMget(cacheKey, "1")
	}
	if fromCache["1"] == nil && redisCache != nil {
		fromCache, err = redisCache.HMget(cacheKey, "1")
		if err != nil {
			return false, err
		}
	}
	var id uint64
	if fromCache["1"] == nil {
		results, _ := searchIds(Where, &Pager{CurrentPage: 1, PageSize: 1}, false, entityType)
		l := len(results)
		value := fmt.Sprintf("%d", l)
		if l > 0 {
			id = results[0]
			value += fmt.Sprintf(" %d", results[0])
		}
		fields := map[string]interface{}{"1": value}
		if localCache != nil {
			if clear {

			} else {
				localCache.HMset(cacheKey, fields)
			}
		}
		if redisCache != nil {
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
		return true, GetById(id, entity)
	}
	return false, nil

}
