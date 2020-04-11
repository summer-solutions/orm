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
	name := r.queueName + "_queue"
	redis, has := r.engine.GetRedis(name)
	if !has {
		return 0, RedisCachePoolNotRegisteredError{Name: name}
	}
	return redis.SCard("dirty_queue")
}

func (r *FlushFromCacheReceiver) Digest() (has bool, err error) {
	name := r.queueName + "_queue"
	cache, has := r.engine.GetRedis(name)
	if !has {
		return false, RedisCachePoolNotRegisteredError{Name: name}
	}
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
	t, has := r.engine.config.getEntityType(val[0])
	if !has {
		return false, EntityNotRegisteredError{Name: val[0]}
	}
	schema := getTableSchema(r.engine.config, t)
	cacheEntity, hasRedis := schema.GetRedisCacheContainer(r.engine)
	if !hasRedis {
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

	err = fillFromDBRow(id, r.engine, decoded, entityValue, schema.t)
	if err != nil {
		return true, err
	}
	entityDBValue := reflect.New(schema.t)
	found, err := searchRow(false, r.engine, NewWhere("`ID` = ?", id), entityDBValue)
	if err != nil {
		return true, err
	}
	if !found {
		return true, err
	}
	ormFieldCache := entityElem.Field(0).Interface().(*ORM)
	ormFieldCache.elem = entityElem
	ormFieldDB, err := r.engine.initIfNeeded(entityDBValue)
	if err != nil {
		return false, err
	}
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
	db := schema.GetMysql(r.engine)

	redisQueueName := "default"
	redisQueue, has := r.engine.getRedisForQueue(redisQueueName)
	if !has {
		return false, RedisCachePoolNotRegisteredError{Name: redisQueueName}
	}
	sql := fmt.Sprintf("UPDATE %s SET %s WHERE `ID` = ?", schema.TableName, strings.Join(fields, ","))
	_, err = db.Exec(sql, attributes...)
	if err != nil {
		_, _ = redisQueue.SAdd("dirty_queue", createDirtyQueueMember(val[0], id))
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
			_, _ = redisQueue.SAdd("dirty_queue", createDirtyQueueMember(val[0], id))
			return true, err
		}
	}
	return true, nil
}
