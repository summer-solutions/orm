package orm

import (
	"encoding/json"
	"fmt"
)

type LazyReceiver struct {
	RedisName string
}

func (r LazyReceiver) Digest() error {
	redis := GetRedisCache(r.RedisName)
	key := "lazy_queue"
	for {
		val, found := redis.RPop(key)
		if !found {
			break
		}
		var data interface{}
		err := json.Unmarshal([]byte(val), &data)
		if err != nil {
			return fmt.Errorf("invalid json: %s", val)
		}
		validMap, ok := data.(map[string]interface{})
		if !ok {
			return fmt.Errorf("invalid map: %v", data)
		}
		err = r.handleQueries(validMap)
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *LazyReceiver) handleQueries(validMap map[string]interface{}) error {
	queries, has := validMap["q"]
	if has {
		validQueries, ok := queries.([]interface{})
		if !ok {
			return fmt.Errorf("invalid queries: %v", queries)
		}
		for _, query := range validQueries {
			validInsert, ok := query.([]interface{})
			if !ok {
				return fmt.Errorf("invalid query: %v", validInsert)
			}
			db := GetMysqlDB(validInsert[0].(string))
			sql := validInsert[1].(string)
			attributes := validInsert[2].([]interface{})
			_, err := db.Exec(sql, attributes...)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
