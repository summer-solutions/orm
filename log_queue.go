package orm

import (
	"time"
)

type LogQueueValue struct {
	PoolName  string
	TableName string
	ID        uint64
	Meta      map[string]interface{}
	Data      map[string]interface{}
	Updated   time.Time
}
