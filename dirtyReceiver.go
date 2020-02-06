package orm

import (
	"fmt"
	"github.com/go-redis/redis/v7"
	"strconv"
	"strings"
	"time"
)

type DirtyReceiver struct {
	QueueCode string
}

type DirtyData struct {
	TableSchema *TableSchema
	Id          uint64
	Inserted    bool
	Updated     bool
	Deleted     bool
}

type DirtyHandler func([]DirtyData) (invalid []*redis.Z, err error)

func (r DirtyReceiver) Size() (int64, error) {
	red, err := r.getRedis()
	if err != nil {
		return 0, err
	}
	return red.ZCard(r.QueueCode)
}

func (r DirtyReceiver) GetEntities() []string {
	results := make([]string, 0)
Exit:
	for name, t := range entities {
		schema := getTableSchema(t)
		for _, tags := range schema.tags {
			queues, has := tags["dirty"]
			if !has {
				continue
			}
			queueNames := strings.Split(queues, ",")
			for _, queueName := range queueNames {
				if r.QueueCode == queueName {
					results = append(results, name)
					continue Exit
				}

			}
		}
	}
	return results
}

func (r DirtyReceiver) Digest(max int, handler DirtyHandler) (has bool, err error) {
	cache, err := r.getRedis()
	if err != nil {
		return false, err
	}
	values, err := cache.ZPopMin(r.QueueCode, int64(max))
	if err != nil {
		return false, err
	}
	if len(values) == 0 {
		return false, nil
	}
	results := make([]DirtyData, 0, len(values))
	for _, v := range values {
		val := strings.Split(v.Member.(string), ":")
		if len(val) != 3 {
			continue
		}
		tableSchema := getTableSchema(GetEntityType(val[0]))
		id, err := strconv.ParseUint(val[2], 10, 64)
		if err != nil {
			continue
		}
		data := DirtyData{
			TableSchema: tableSchema,
			Id:          id,
			Inserted:    val[1] == "i",
			Updated:     val[1] == "u",
			Deleted:     val[1] == "d",
		}
		results = append(results, data)
	}
	invalid, err := handler(results)
	if err != nil {
		if invalid != nil {
			_, _ = cache.ZAdd(r.QueueCode, invalid...)
		}
		return true, err
	}
	return true, nil
}

func (r DirtyReceiver) MarkDirty(entityName string, ids ...uint64) error {
	cache, err := r.getRedis()
	if err != nil {
		return err
	}
	data := make([]*redis.Z, len(ids))
	for index, id := range ids {
		data[index] = &redis.Z{Score: float64(time.Now().Unix()), Member: fmt.Sprintf("%s:%d", entityName, id)}
	}
	_, err = cache.ZAdd(r.QueueCode, data...)
	return err
}

func (r DirtyReceiver) getRedis() (*RedisCache, error) {
	redisCode, has := dirtyQueuesCodes[r.QueueCode]
	if !has {
		return nil, fmt.Errorf("unregistered dirty queue %s", r.QueueCode)
	}
	return GetRedis(redisCode), nil
}
