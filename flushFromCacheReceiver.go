package orm

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strconv"
	"strings"
)

type FlushFromCacheReceiver struct {
	engine    *Engine
	queueName string
}

func NewFlushFromCacheReceiver(engine *Engine, queueName string) *FlushFromCacheReceiver {
	return &FlushFromCacheReceiver{engine: engine, queueName: queueName}
}

func (r *FlushFromCacheReceiver) Size() (int64, error) {
	return r.engine.GetRedis(r.queueName + "_queue").SCard("dirty_queue")
}

func (r *FlushFromCacheReceiver) Digest(engine *Engine) (has bool, err error) {
	cache := r.engine.GetRedis(r.queueName + "_queue")
	value, has, err := cache.SPop("dirty_queue")
	if err != nil {
		return false, err
	}
	if !has {
		return false, nil
	}
	val := strings.Split(value, ":")
	if len(val) != 2 {
		return true, nil
	}
	id, err := strconv.ParseUint(val[1], 10, 64)
	if err != nil {
		return true, err
	}
	schema := getTableSchema(engine.config, r.engine.config.GetEntityType(val[0]))
	cacheEntity := schema.GetRedisCacheContainer(engine)
	if cacheEntity == nil {
		return true, nil
	}
	cacheKey := schema.getCacheKey(id)
	inCache, has, err := cacheEntity.Get(cacheKey)
	if err != nil {
		return true, err
	}
	if !has {
		return true, err
	}
	entityValue := reflect.New(schema.t)
	entityElem := entityValue.Elem()

	var decoded []string
	err = json.Unmarshal([]byte(inCache), &decoded)
	if err != nil {
		return true, err
	}

	err = fillFromDBRow(engine, decoded, entityValue, schema.t)
	if err != nil {
		return true, err
	}
	entityDBValue := reflect.New(schema.t)
	found, err := searchRow(engine, NewWhere("`Id` = ?", id), entityDBValue)
	if err != nil {
		return true, err
	}
	if !found {
		return true, err
	}
	ormFieldCache := entityElem.Field(0).Interface().(*ORM)
	ormFieldCache.elem = entityElem
	ormFieldDB := engine.initIfNeeded(entityDBValue)
	newData := make(map[string]interface{}, len(ormFieldCache.dBData))
	for k, v := range ormFieldCache.dBData {
		newData[k] = v
	}
	for k, v := range ormFieldDB.dBData {
		ormFieldCache.dBData[k] = v
	}
	is, bind, err := isDirty(entityElem)
	if err != nil {
		return true, err
	}
	if !is {
		return true, nil
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
	db := schema.GetMysql(engine)
	sql := fmt.Sprintf("UPDATE %s SET %s WHERE `Id` = ?", schema.TableName, strings.Join(fields, ","))
	_, err = db.Exec(sql, attributes...)
	if err != nil {
		_, _ = engine.getRedisForQueue("default").SAdd("dirty_queue", createDirtyQueueMember(val[0], id))
		return true, err
	}
	cacheKeys, err := getCacheQueriesKeys(schema, bind, ormFieldCache.dBData, false)
	if err != nil {
		return false, err
	}
	keys, err := getCacheQueriesKeys(schema, bind, newData, false)
	if err != nil {
		return false, err
	}
	cacheKeys = append(cacheKeys, keys...)
	if len(cacheKeys) > 0 {
		err = cacheEntity.Del(cacheKeys...)
		if err != nil {
			_, _ = engine.getRedisForQueue("default").SAdd("dirty_queue", createDirtyQueueMember(val[0], id))
			return true, err
		}
	}
	return true, nil
}
