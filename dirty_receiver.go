package orm

import (
	"reflect"
	"strconv"
	"strings"
)

type DirtyReceiver struct {
	RedisName string
}

func (r DirtyReceiver) Size() int64 {
	return GetRedisCache(r.RedisName).ZCard("dirty_queue")
}

func (r DirtyReceiver) Digest() error {
	cache := GetRedisCache(r.RedisName)
	for {
		values := cache.ZPopMin("dirty_queue", 1)
		if len(values) == 0 {
			break
		}
		val := strings.Split(values[0].Member.(string), ":")
		if len(val) != 2 {
			continue
		}
		id, err := strconv.ParseUint(val[1], 10, 64)
		if err != nil {
			continue
		}
		schema := GetTableSchema(val[0])
		cache := schema.GetRedisCacheContainer()
		if cache == nil {
			continue
		}
		cacheKey := schema.getCacheKey(id)
		inCache, ok := cache.Get(cacheKey)
		if !ok {
			continue
		}
		entityInCache := createEntityFromDBRow(inCache, schema.t)
		entityInDB := SearchOne(NewWhere("`Id` = ?", id), NewPager(1, 1), val[0])
		if entityInDB == nil {
			continue
		}
		ormFieldCache := reflect.ValueOf(entityInCache).Field(0).Interface().(ORM)
		ormFieldDB := reflect.ValueOf(entityInDB).Field(0).Interface().(ORM)
		for k, v := range ormFieldDB.DBData {
			ormFieldCache.DBData[k] = v
		}
		is, _ := IsDirty(entityInCache)
		if !is {
			continue
		}
		err = flush(false, true, &entityInCache)
		if err != nil {
			GetRedisCache(queueRedisName).ZAdd("dirty_queue", createDirtyQueueMember(val[0], id))
			return err
		}
	}
	return nil
}
