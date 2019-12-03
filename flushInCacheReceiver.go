package orm

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type FlushInCacheReceiver struct {
	RedisName string
}

func (r FlushInCacheReceiver) Size() (int64, error) {
	return GetRedis(r.RedisName).ZCard("dirty_queue")
}

func (r FlushInCacheReceiver) Digest() error {
	cache := GetRedis(r.RedisName)
	for {
		values, err := cache.ZPopMin("dirty_queue", 1)
		if err != nil {
			return err
		}
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
		schema := getTableSchema(getEntityType(val[0]))
		cache := schema.GetRedisCacheContainer()
		if cache == nil {
			continue
		}
		cacheKey := schema.getCacheKey(id)
		inCache, has, err := cache.Get(cacheKey)
		if err != nil {
			return err
		}
		if !has {
			continue
		}
		entityInCache := reflect.New(schema.t).Elem()
		err = fillFromDBRow(inCache, entityInCache, schema.t)
		if err != nil {
			return err
		}
		entityDBValue := reflect.New(schema.t).Elem()
		found, err := searchRow(NewWhere("`Id` = ?", id), schema.t, entityDBValue)
		if err != nil {
			return err
		}
		if !found {
			continue
		}
		ormFieldCache := entityInCache.Field(0).Interface().(*ORM)
		ormFieldCache.e = &entityInCache
		ormFieldDB, err := initIfNeeded(entityDBValue, &entityInCache)
		if err != nil {
			return err
		}
		newData := make(map[string]interface{}, len(ormFieldCache.dBData))
		for k, v := range ormFieldCache.dBData {
			newData[k] = v
		}
		for k, v := range ormFieldDB.dBData {
			ormFieldCache.dBData[k] = v
		}
		is, bind, err := ormFieldCache.isDirty(entityInCache)
		if err != nil {
			return err
		}
		if !is {
			continue
		}

		bindLength := len(bind)
		fields := make([]string, bindLength)
		attributes := make([]interface{}, bindLength+1)
		i := 0
		for key, value := range bind {
			fields[i] = fmt.Sprintf("`%s` = ?", key)
			attributes[i] = value
			i++
		}
		attributes[i] = id
		db := schema.GetMysql()
		sql := fmt.Sprintf("UPDATE %s SET %s WHERE `Id` = ?", schema.TableName, strings.Join(fields, ","))
		_, err = db.Exec(sql, attributes...)
		if err != nil {
			_, _ = getRedisForQueue("default").ZAdd("dirty_queue", createDirtyQueueMember(val[0], id))
			return err
		}
		cacheKeys := getCacheQueriesKeys(schema, bind, ormFieldCache.dBData, false)
		cacheKeys = append(cacheKeys, getCacheQueriesKeys(schema, bind, newData, false)...)
		if len(cacheKeys) > 0 {
			err = cache.Del(cacheKeys...)
			if err != nil {
				_, _ = getRedisForQueue("default").ZAdd("dirty_queue", createDirtyQueueMember(val[0], id))
				return err
			}
		}
	}
	return nil
}
