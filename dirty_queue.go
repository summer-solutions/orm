package orm

type DirtyQueueValue struct {
	EntityName string
	ID         uint64
	Added      bool
	Updated    bool
	Deleted    bool
}
