package orm

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"

	jsoniter "github.com/json-iterator/go"
)

const flushCacheQueueName = "orm_flush_cache"

type FlushFromCacheReceiver struct {
	engine      *Engine
	disableLoop bool
	heartBeat   func()
}

func NewFlushFromCacheReceiver(engine *Engine) *FlushFromCacheReceiver {
	return &FlushFromCacheReceiver{engine: engine}
}

func (r *FlushFromCacheReceiver) DisableLoop() {
	r.disableLoop = true
}

func (r *FlushFromCacheReceiver) SetHeartBeat(beat func()) {
	r.heartBeat = beat
}

func (r *FlushFromCacheReceiver) Digest() {
	channel := r.engine.GetRabbitMQQueue(flushCacheQueueName)
	consumer := channel.NewConsumer("default consumer")
	defer consumer.Close()
	if r.disableLoop {
		consumer.DisableLoop()
	}
	if r.heartBeat != nil {
		consumer.SetHeartBeat(r.heartBeat)
	}
	consumer.Consume(func(items [][]byte) {
		for _, item := range items {
			val := strings.Split(string(item), ":")
			id, _ := strconv.ParseUint(val[1], 10, 64)
			t, has := r.engine.registry.entities[val[0]]
			if !has {
				continue
			}
			schema := getTableSchema(r.engine.registry, t)
			cacheEntity, _ := schema.GetRedisCache(r.engine)
			cacheKey := schema.getCacheKey(id)
			inCache, has := cacheEntity.Get(cacheKey)
			if !has {
				continue
			}
			entityValue := reflect.New(schema.t)
			entity := entityValue.Interface().(Entity)

			var decoded []string
			_ = jsoniter.ConfigFastest.Unmarshal([]byte(inCache), &decoded)

			fillFromDBRow(id, r.engine, decoded, entity)
			entityDBValue := reflect.New(schema.t).Interface().(Entity)
			_ = searchRow(false, r.engine, NewWhere("`ID` = ?", id), entityDBValue, nil)
			newData := make(map[string]interface{}, len(entity.getORM().dBData))
			for k, v := range entity.getORM().dBData {
				newData[k] = v
			}
			for k, v := range entityDBValue.getORM().dBData {
				entity.getORM().dBData[k] = v
			}
			is, bind := getDirtyBind(entity)
			if !is {
				return
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
			_ = db.Exec(sql, attributes...)
			cacheKeys := getCacheQueriesKeys(schema, bind, entity.getORM().dBData, false)

			keys := getCacheQueriesKeys(schema, bind, newData, false)
			cacheKeys = append(cacheKeys, keys...)
			if len(cacheKeys) > 0 {
				cacheEntity.Del(cacheKeys...)
			}
		}
	})
}
