package orm

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	jsoniter "github.com/json-iterator/go"
)

type FlushFromCacheReceiver struct {
	engine    *Engine
	queueName string
}

func NewFlushFromCacheReceiver(engine *Engine, queueName string) *FlushFromCacheReceiver {
	return &FlushFromCacheReceiver{engine: engine, queueName: queueName}
}

func (r *FlushFromCacheReceiver) Size() (int64, error) {
	name := r.queueName + "_queue"
	redis := r.engine.GetRedis(name)
	return redis.SCard("dirty_queue")
}

func (r *FlushFromCacheReceiver) Digest() (has bool, err error) {
	name := r.queueName + "_queue"
	cache := r.engine.GetRedis(name)
	value, has, err := cache.SPop("dirty_queue")
	if err != nil || !has {
		return false, err
	}
	val := strings.Split(value, ":")
	id, _ := strconv.ParseUint(val[1], 10, 64)
	t, has := r.engine.registry.entities[val[0]]
	if !has {
		return true, nil
	}
	schema := getTableSchema(r.engine.registry, t)
	cacheEntity, _ := schema.GetRedisCache(r.engine)
	cacheKey := schema.getCacheKey(id)
	inCache, has, _ := cacheEntity.Get(cacheKey)
	if !has {
		return true, nil
	}
	entityValue := reflect.New(schema.t)
	entity := entityValue.Interface().(Entity)

	var decoded []string
	_ = jsoniter.ConfigCompatibleWithStandardLibrary.Unmarshal([]byte(inCache), &decoded)

	err = fillFromDBRow(id, r.engine, decoded, entity)
	if err != nil {
		return true, err
	}
	entityDBValue := reflect.New(schema.t).Interface().(Entity)
	found, err := searchRow(false, r.engine, NewWhere("`ID` = ?", id), entityDBValue, nil)
	if err != nil || !found {
		return true, err
	}
	newData := make(map[string]interface{}, len(entity.getORM().dBData))
	for k, v := range entity.getORM().dBData {
		newData[k] = v
	}
	for k, v := range entityDBValue.getORM().dBData {
		entity.getORM().dBData[k] = v
	}
	is, bind, err := getDirtyBind(entity)
	if err != nil || !is {
		return true, err
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
	db := schema.GetMysql(r.engine)

	/* #nosec */
	sql := fmt.Sprintf("UPDATE %s SET %s WHERE `ID` = ?", schema.tableName, strings.Join(fields, ","))
	_, err = db.Exec(sql, attributes...)
	if err != nil {
		return true, err
	}
	cacheKeys := getCacheQueriesKeys(schema, bind, entity.getORM().dBData, false)

	keys := getCacheQueriesKeys(schema, bind, newData, false)
	cacheKeys = append(cacheKeys, keys...)
	if len(cacheKeys) > 0 {
		err = cacheEntity.Del(cacheKeys...)
	}
	return true, err
}
