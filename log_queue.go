package orm

import (
	"time"
)

type LogQueueValue struct {
	PoolName  string
	TableName string
	ID        uint64
	Meta      map[string]interface{}
	Before    map[string]interface{}
	Changes   map[string]interface{}
	Updated   time.Time
}
