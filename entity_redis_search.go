package orm

func (e *Engine) RedisSearchIds(entity Entity, pager *Pager, references ...string) (totalRows int, ids []uint64) {
	redisSearch(e, entity, pager, references)
	return 0, nil
}

func redisSearch(e *Engine, entity Entity, pager *Pager, references []string) {

}
