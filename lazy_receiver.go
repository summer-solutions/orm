package orm

import (
	"encoding/json"
	"fmt"
)

type LazyReceiver struct {
	RedisName string
}

func (r LazyReceiver) Digest() int {
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
			panic(fmt.Errorf("invalid json: %s", val))
		}
		validMap, ok := data.(map[string]interface{})
		if !ok {
			panic(fmt.Errorf("invalid map: %v", data))
		}
		inserts, has := validMap["i"]
		if has {
			validInserts, ok := inserts.([]interface{})
			if !ok {
				panic(fmt.Errorf("invalid inserts: %v", inserts))
			}
			for _, insert := range validInserts {
				validInsert, ok := insert.([]interface{})
				if !ok {
					panic(fmt.Errorf("invalid inserts: %v", validInsert))
				}
				db := GetMysqlDB(validInsert[0].(string))
				sql := validInsert[1].(string)
				attributes := validInsert[2].([]interface{})
				_, err := db.Exec(sql, attributes...)
				if err != nil {
					panic(err.Error())
				}
			}
		}
	}
	return 0
}
