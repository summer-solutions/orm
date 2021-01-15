package orm

import (
	"strconv"
)

type DirtyEntityEvent interface {
	ID() uint64
	TableSchema() TableSchema
	Added() bool
	Updated() bool
	Deleted() bool
}

func EventDirtyEntity(e Event) DirtyEntityEvent {
	data := e.RawData()
	id, _ := strconv.ParseUint(data["I"].(string), 10, 64)
	action := data["A"].(string)
	schema := e.(*event).consumer.redis.engine.registry.GetTableSchema(data["E"].(string))
	return &dirtyEntityEvent{id: id, schema: schema, added: action == "i", updated: action == "u", deleted: action == "d"}
}

type dirtyEntityEvent struct {
	id      uint64
	added   bool
	updated bool
	deleted bool
	schema  TableSchema
}

func (d *dirtyEntityEvent) ID() uint64 {
	return d.id
}

func (d *dirtyEntityEvent) TableSchema() TableSchema {
	return d.schema
}

func (d *dirtyEntityEvent) Added() bool {
	return d.added
}

func (d *dirtyEntityEvent) Updated() bool {
	return d.updated
}

func (d *dirtyEntityEvent) Deleted() bool {
	return d.deleted
}
