package orm

func getDirtyBind(entity Entity) (is bool, bind map[string]interface{}) {
	orm := entity.getORM()
	if orm.attributes.delete {
		return true, nil
	}
	id := orm.GetID()
	t := orm.attributes.elem.Type()
	bind = createBind(id, orm.tableSchema, t, orm.attributes.elem, orm.dBData, "")
	is = id == 0 || len(bind) > 0
	return is, bind
}
